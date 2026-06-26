from typing import Callable

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from utils.decorators import safe_step

from transformers.helpers import _clean
from config.pipeline_config import DEFAULT_FALLBACK_VALUES
from transformers.constants import (
    NULL_LIKE_VALUES,
    STRING_COLS,
    BOOL_COLS,
    TEMP_COLS,
)


@safe_step("Drop and Rename Columns")
def drop_and_rename(config: dict) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Renames raw columns and drops unnecessary ones to match standard schema.
    
    Args:
        config (dict): Seller-specific configuration. 
                       Expects 'keep_csv_product_seller' (bool) to determine 
                       if the original seller column should be preserved.
                       
    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function.
        
    Raises:
        SchemaMismatchError: If expected columns are missing in the DataFrame.
        ComputationError: For any other errors during transformation.

    Output Schema:
        - product_name: string
        - product_seller: string
    
    Process Description:
        1. Renames raw columns to match standard schema.
        2. Drops unnecessary columns.
        3. Preserves original seller column if specified.
        4. Assigns seller name if not preserved.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'keep_csv_product_seller' key which determines if the original seller column should be preserved.
    """

    keep_csv_seller = config.get('keep_csv_product_seller', False)
    seller_name = config.get('seller_name', 'unknown_seller')

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        if keep_csv_seller:
            df = df.drop('seller')
        else:
            df = df.drop('product_seller', 'seller')
            df = df.withColumn('product_seller', F.lit(seller_name))

        return df.withColumnRenamed('title', 'product_name').drop('name', 'product_unit')

    return transform


@safe_step("Fill Null Values")
def fill_nulls(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Fills null values in string columns with 'N/A' to ensure consistency.

    Args:
        config (dict, optional): Configuration dictionary for future extensibility. Defaults to None.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function that fills nulls

    Raises:
        ComputationError: If an error occurs during the transformation. 
        SchemaMismatchError: If expected columns are missing in the DataFrame.

    Output Schema:
        - product_has_image_url: boolean
        - product_has_ram: boolean
        - product_has_storage: boolean
        - product_image_url: string
        - product_ram: string
        - product_storage: string
        - product_category: string
        - product_subcategory: string
        - product_brand: string
        - product_name: string
        - product_seller: string
    
    Process Description:
        1. Fills null values in string columns with 'N/A'.
        2. Checks for stock-related keywords in the availability column.
        3. Considers products with a positive current price as available.
        4. Normalizes the product availability status based on the specified strategy in the configuration.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'availability' key which determines the normalization strategy to apply. 
            Supported strategies include 'stock_string', and 'price_positive'.
    """

    config = config or {}
    string_cols = config.get("string_cols", STRING_COLS)
    flag_cols = config.get("flag_cols", BOOL_COLS)
    img_strategy = config.get('image_handling', 'default')

    llm_targets = {'product_category', 'product_subcategory', 'product_brand'}

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        cols = df.columns
        cols_to_update = {}

        img_raw = F.col('product_image_url').cast('string') if 'product_image_url' in cols else F.lit(None).cast(
            'string')
        ram_raw = F.col('product_ram').cast('string') if 'product_ram' in cols else F.lit(None).cast('string')
        storage_raw = F.col('product_storage').cast('string') if 'product_storage' in cols else F.lit(None).cast(
            'string')

        img_logic = {
            'placeholder': F.when(F.lower(img_raw).contains('placeholder'), F.lit(None).cast('string')).otherwise(
                img_raw),
            'unknown': F.when(img_raw == 'unknown', F.lit(None).cast('string')).otherwise(img_raw)
        }.get(img_strategy, img_raw)

        cols_to_update['product_has_image_url'] = F.when(img_logic.isNotNull() & (img_logic != 'N/A'),
                                                         F.lit(1)).otherwise(F.lit(0))
        cols_to_update['product_has_ram'] = F.when(ram_raw.isNotNull() & (F.lower(ram_raw) != 'unknown'),
                                                   F.lit(1)).otherwise(F.lit(0))
        cols_to_update['product_has_storage'] = F.when(storage_raw.isNotNull() & (F.lower(storage_raw) != 'unknown'),
                                                       F.lit(1)).otherwise(F.lit(0))


        cols_to_update['product_image_url'] = F.coalesce(img_logic, F.lit(
            DEFAULT_FALLBACK_VALUES.get('product_image_url', 'missing_url')))
        cols_to_update['product_ram'] = F.coalesce(ram_raw,
                                                   F.lit(DEFAULT_FALLBACK_VALUES.get('product_ram', 'unknown')))
        cols_to_update['product_storage'] = F.coalesce(storage_raw,
                                                       F.lit(DEFAULT_FALLBACK_VALUES.get('product_storage', 'unknown')))

        special_strings = {'product_image_url', 'product_ram', 'product_storage'}

        for c in string_cols:
            if c in cols and c not in special_strings:
                cleaned = _clean(c).cast("string")
                normalized = F.lower(F.trim(cleaned))
                cleaned_final = F.when(
                    cleaned.isNull() | normalized.isin(*NULL_LIKE_VALUES),
                    F.lit(None).cast("string")
                ).otherwise(cleaned)

                if c in llm_targets:
                    cols_to_update[c] = cleaned_final
                else:
                    cols_to_update[c] = F.coalesce(cleaned_final, F.lit(DEFAULT_FALLBACK_VALUES.get(c, "unknown")))

        true_cond  = ["true", "1", "yes", "y", "available", "in stock", "متوفر", "متاح", "موجود", "عندنا"]
        false_cond = ["false", "0", "no", "n", "unavailable", "out of stock", "غير متوفر", "فير متاح", "مش موجود", "مش عندنا"]

        special_flags = {'product_has_image_url', 'product_has_ram', 'product_has_storage'}
        for c in flag_cols:
            if c in cols and c not in special_flags:
                norm_flag = F.lower(F.trim(F.col(c).cast("string")))
                cols_to_update[c] = (
                    F.when(norm_flag.isin(*true_cond), F.lit(1))
                    .when(norm_flag.isin(*false_cond), F.lit(0))
                    .otherwise(F.coalesce(F.col(c).cast("int"), F.lit(0)))
                )

        return df.withColumns(cols_to_update) if cols_to_update else df

    return transform


@safe_step("Drop Temporary Columns")
def drop_temps(df: SparkDataFrame) -> SparkDataFrame:
    """
    Drops temporary columns used during intermediate transformations to clean the DataFrame.

    Args:
        df (SparkDataFrame): The input DataFrame.

    Returns:
        SparkDataFrame: The DataFrame with temporary columns dropped.
        
    Raises:
        SchemaMismatchError: If expected columns are missing in the DataFrame.

    Output Schema:
        - DataFrame with temporary columns dropped
    
    Process Description:
        1. Drops temporary columns from the DataFrame.
        2. Removes duplicate rows from the DataFrame.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'dedup_key' to identify duplicates.
    """

    to_drop = [c for c in TEMP_COLS if c in df.columns]
    return df.drop(*to_drop) if to_drop else df


@safe_step("Remove Duplicate Rows")
def remove_duplicate(config: dict) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Removes duplicate rows from the DataFrame based on a specified key column.

    Args:
        config (dict): Configuration dictionary containing the 'dedup_key' to identify duplicates.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function that removes duplicates.

    Raises:
        SchemaMismatchError: If the specified 'dedup_key' is not present in the DataFrame.
        ComputationError: For any other errors during the transformation.

    Output Schema:
        - DataFrame with duplicate rows removed
    
    Process Description:
        1. Removes duplicate rows from the DataFrame based on a specified key column.
        2. Drops duplicate rows based on the 'dedup_key' column.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'dedup_key' to identify duplicates.
    """

    key = config.get('dedup_key')

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        return df.dropDuplicates([key]) if key and key in df.columns else df

    return transform


@safe_step("Cast Data Types")
def cast_data_types(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Casts specified columns in the DataFrame to their appropriate data types based on predefined rules.

    Args:
        config (dict, optional): Configuration dictionary (Optional), for any future updates. Defaults to None.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function that casts data types.

    Raises:
        SchemaMismatchError: If expected columns are missing in the DataFrame.
        ComputationError: For any other errors during the transformation.

    Output Schema:
        - DataFrame with columns cast to appropriate data types
    
    Process Description:
        1. Casts specified columns in the DataFrame to their appropriate data types based on predefined rules.
        2. Uses 'cast' method to cast columns to their appropriate data types.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'string_cols' and 'bool_cols' to identify columns to cast.
    """

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        cast_dict = {
            **{c: F.col(c).cast("string") for c in STRING_COLS if c in df.columns},
            **{c: F.col(c).cast("int") for c in BOOL_COLS if c in df.columns}
        }

        return df.withColumns(cast_dict) if cast_dict else df

    return transform


@safe_step("Drop Unneeded Columns")
def drop_unneeded_columns(df: SparkDataFrame) -> SparkDataFrame:
    """
    Drops unneeded columns from the DataFrame to streamline the dataset.

    Args:
        df (SparkDataFrame): The input DataFrame.

    Returns:
        SparkDataFrame: The DataFrame with unneeded columns dropped.

    Raises:
        SchemaMismatchError: If expected columns are missing in the DataFrame.
        ComputationError: For any other errors during the transformation.

    Output Schema:
        - DataFrame with unneeded columns dropped
    
    Process Description:
        1. Drops unneeded columns from the DataFrame.
        2. Removes duplicate rows from the DataFrame.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'dedup_key' to identify duplicates.
    """

    cols = frozenset(["is_business_day", "datetimezone", "timestamp", ""])
    to_drop = [c for c in cols if c in df.columns]

    return df.drop(*to_drop) if to_drop else df