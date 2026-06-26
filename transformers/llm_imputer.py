from __future__ import annotations

import os
import json
import shutil
from typing import Callable

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.session import SparkSession

from logs.logger import get_logger
from utils.decorators import safe_step
from agents.FailoverManager import FailoverManager

from config.pipeline_config import DEFAULT_FALLBACK_VALUES
from transformers.constants import (
    NULL_LIKE_VALUES,
    _TARGETS,
    DEFAULT_CACHE_PATH
)

logger = get_logger(__name__)

_failover_manager = FailoverManager()


def _compact_cache_dir(spark: SparkSession, cache_path: str):
    if not os.path.exists(cache_path): return
    try:
        df = spark.read.parquet(cache_path)
        if df.isEmpty(): return
        temp_path = cache_path + "_temp"
        
        num_partitions = max(1, df.count() // 100000) 
        
        df.dropDuplicates(["input_key"]).repartition(num_partitions).write.mode("overwrite").parquet(temp_path)
        spark.read.parquet(temp_path).write.mode("overwrite").parquet(cache_path)
        if os.path.exists(temp_path): shutil.rmtree(temp_path)
    except Exception as e:
        logger.error(f"Cache Compaction Error: {e}")


def _normalize_product_names(df: SparkDataFrame, input_col: str = "product_name", output_col: str = "normalized_name") -> SparkDataFrame:
    normalized = F.lower(F.trim(F.coalesce(F.col(input_col).cast("string"), F.lit(""))))
    normalized = F.regexp_replace(normalized, r"&amp;amp;|&amp;", "&")
    normalized = F.regexp_replace(normalized, r"\+", " plus ")
    normalized = F.regexp_replace(normalized, r"[^\w\s&'\u0600-\u06FF]", " ")
    normalized = F.regexp_replace(normalized, r"\s+", " ")
    return df.withColumn(output_col, F.trim(normalized))


def _add_classification_signature(df: SparkDataFrame) -> SparkDataFrame:
    signature = F.col("normalized_name")
    signature = F.regexp_replace(signature, r"\b\d+(?:\.\d+)?\s*(?:kg|g|gm|ml|l|m|cm|mm|inch|oz)\b", " ")
    signature = F.regexp_replace(signature, r"\b(?:pack|set|bundle|box)\s*(?:of)?\s*\d+\b", " ")
    signature = F.regexp_replace(signature, r"\b\d+(?:\.\d+)?\b", " ")
    signature = F.regexp_replace(signature, r"\s+", " ")
    return df.withColumn("classification_key", F.when(F.trim(signature) == "", F.col("normalized_name")).otherwise(F.trim(signature)))


def _cache_key_col(col_name: str):
    if col_name in {"product_category", "product_subcategory"}:
        return F.col("classification_key")
    return F.col("normalized_name")


def _prepare_cache_for_lookup(cache_df: SparkDataFrame, col_name: str) -> SparkDataFrame:
    cache_df = _add_classification_signature(_normalize_product_names(cache_df, "input_key", "normalized_name"))
    return cache_df.withColumn("cache_key", _cache_key_col(col_name)).select("cache_key", "predicted_value").filter(F.col("cache_key") != "").dropDuplicates(["cache_key"])


def _missing_cond(col_name: str):
    value = F.trim(F.col(col_name).cast("string"))
    return F.col(col_name).isNull() | (value == "") | F.lower(value).isin(*NULL_LIKE_VALUES)


def _process_ai_fallback(spark: SparkSession, needs_ai_df: SparkDataFrame, col_name: str, dataset_name: str, cache_path: str, batch_size: int):
    keys_and_names = needs_ai_df.select("cache_key", "product_name").distinct().collect()
    
    batch = []
    for row in keys_and_names:
        if not row["cache_key"]: continue
        batch.append({"input_key": row["cache_key"], "product_name": row["product_name"]})
        
        if len(batch) >= batch_size:
            _call_agent(spark, batch, col_name, dataset_name, cache_path)
            batch = []
            
    if batch:
        _call_agent(spark, batch, col_name, dataset_name, cache_path)


def _call_agent(spark: SparkSession, batch: list, col_name: str, dataset_name: str, cache_path: str):
    try:
        prompt = (
            f"Dataset: {dataset_name}. Task: Predict the English standard value for '{col_name}'.\n"
            f"Note: The input 'product_name' might be in Arabic, English, or mixed. You must understand the meaning and output the standard taxonomy value in English.\n"
            f"Return a strict JSON array of objects with 'input_key' and 'predicted_value'.\n"
            f"Inputs:\n{json.dumps(batch, ensure_ascii=False)}"
        )
        
        resp = _failover_manager.predict_missing_value(prompt)
        if not resp: return
        
        parsed = json.loads(resp.strip().strip("`").removeprefix("json").strip())
        raw_preds = parsed if isinstance(parsed, list) else (list(parsed.values())[0] if isinstance(parsed, dict) else [parsed])
        
        if raw_preds:
            df_preds = spark.createDataFrame(raw_preds, schema="input_key STRING, predicted_value STRING").dropna(subset=["input_key"])
            df_preds.write.mode("append").parquet(cache_path)
            
    except Exception as e:
        logger.error(f"AI Agent Batch Error: {e}")


@safe_step("LLM Imputation Pipeline - Pure AI Agent Edition")
def fill_na_with_llm(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    This function fills missing values in the product category, product subcategory, and product brand columns using an LLM.

    Args:
        config (dict): Configuration dictionary containing llm_cache_path, llm_batch_size, and seller_name.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A function that takes a SparkDataFrame and returns a SparkDataFrame with missing values filled.

    Requires:
        - Google GenAI API key
        - SparkSession
        - PySpark Dataframe
        - `config` dictionary with `llm_cache_path`, `llm_batch_size`, and `seller_name`.

    Process Description:
        1. Normalizes product names and creates a classification key.
        2. For each target column, identifies missing values.
        3. Uses a cache to store and retrieve previously predicted values.
        4. Calls an AI agent to predict missing values for unseen data.
        5. Merges the predicted values back into the DataFrame.
    
    Output Columns:
        - product_category
        - product_subcategory
        - product_brand
    """
    config = config or {}
    cache_base = config.get("llm_cache_path", DEFAULT_CACHE_PATH)
    batch_size = config.get("llm_batch_size", 50)
    dataset_name = config.get("seller_name", "Unknown").upper()

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        spark = df.sparkSession
        if "product_name" not in df.columns: return df

       
        df = _add_classification_signature(_normalize_product_names(df))

        for col_name in _TARGETS:
            if col_name not in df.columns: continue
            missing_cond = _missing_cond(col_name)
            if df.filter(missing_cond).isEmpty(): continue

            fallback_value = DEFAULT_FALLBACK_VALUES.get(col_name, "unknown")
            logger.info(f"[{dataset_name}] | {col_name} | Processing via AI Agent Pipeline...")

            unique_missing = (
                df.filter(missing_cond)
                .withColumn("cache_key", _cache_key_col(col_name))
                .select("product_name", "normalized_name", "classification_key", "cache_key")
                .filter(F.col("cache_key") != "")
                .distinct()
                .cache()
            )
            
            cache_path = os.path.join(cache_base, col_name)
            cache_df = spark.read.parquet(cache_path).dropDuplicates(["input_key"]) if os.path.exists(cache_path) else spark.createDataFrame([], schema="input_key STRING, predicted_value STRING")
            cache_lookup_df = _prepare_cache_for_lookup(cache_df, col_name)

            
            unseen_df = unique_missing.join(cache_lookup_df.select("cache_key").distinct(), "cache_key", "left_anti")
            
            
            if not unseen_df.isEmpty():
                try:
                    _process_ai_fallback(spark, unseen_df, col_name, dataset_name, cache_path, batch_size)
                finally:
                    pass
            
            unique_missing.unpersist()
            
            
            if config.get("compact_llm_cache", False):
                _compact_cache_dir(spark, cache_path)

           
            if os.path.exists(cache_path):
                spark.catalog.refreshByPath(cache_path)
                final_cache = _prepare_cache_for_lookup(spark.read.parquet(cache_path).dropDuplicates(["input_key"]), col_name)
                df = df.withColumn("cache_key", _cache_key_col(col_name))
                df = df.join(final_cache, "cache_key", "left")
                df = df.withColumn(
                    col_name, 
                    F.coalesce(F.when(~missing_cond, F.col(col_name)), F.col("predicted_value"), F.lit(fallback_value))
                ).drop("cache_key", "predicted_value")

        
        cols_to_drop = ["normalized_name", "classification_key", "PRODUCT_BRAND_SOURCE", "product_brand_source"]
        for col in cols_to_drop:
            if col in df.columns:
                df = df.drop(col)
                
        return df
    return transform