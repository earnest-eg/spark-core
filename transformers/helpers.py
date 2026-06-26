import re
import html
from typing import Callable

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from transformers.constants import UNIT_MAP, BRAND_NOISE_VALUES, NULL_LIKE_VALUES


def _clean(col_expr) -> F.Column:
    """Normalize null-like string values to real null."""
    expr = F.col(col_expr) if isinstance(col_expr, str) else col_expr
    cleaned = F.lower(F.trim(expr.cast("string")))

    return F.when(
        expr.isNull() | cleaned.isin(*NULL_LIKE_VALUES),
        F.lit(None).cast("string")
    ).otherwise(expr)


def _safe_double(col_expr) -> F.Column:
    """Safely cast to double, replacing empty/null strings with None."""
    return _clean(col_expr).cast('double')


def _normalize_text(col_expr) -> F.Column:
    """
    Centralized text normalization: lowers, handles ampersands, 
    removes specific punctuation, and condenses spaces.
    """
    c = F.col(col_expr).cast("string") if isinstance(col_expr, str) else col_expr.cast("string")
    c = F.lower(F.trim(c))
    c = F.regexp_replace(c, r"&amp;(amp;)?", "&")
    c = F.regexp_replace(c, r"\+", " plus ")
    c = F.regexp_replace(c, r"[/_,|\-]+", " ")
    c = F.regexp_replace(c, r"\s*&\s*", " & ")
    c = F.regexp_replace(c, r"[^\w\s&']", " ")
    c = F.regexp_replace(c, r"\s+", " ")

    return F.when(
        c.isNull() | c.isin(*NULL_LIKE_VALUES),
        F.lit(None).cast("string")
    ).otherwise(F.trim(c))



def apply_broadcast_mapping(df: SparkDataFrame, mapping_dict: dict, input_col: str, output_col: str) -> SparkDataFrame:
    """
    Replaces massive F.create_map() calls.
    Converts dict to a DataFrame and broadcasts it for an ultra-fast, memory-efficient Join.
    Acts like a shared pointer to reference data across cluster nodes.
    """
    if not mapping_dict:
        return df.withColumn(output_col, F.lit(None).cast("string"))

    spark = df.sparkSession

    mapping_data = [(str(k).lower().strip(), str(v)) for k, v in mapping_dict.items()]
    mapping_df = spark.createDataFrame(mapping_data, ["_match_key", f"_{output_col}_mapped"])
    mapping_df = F.broadcast(mapping_df)

    df = df.withColumn("_lookup_key", F.lower(F.trim(F.col(input_col))))
    df = df.join(mapping_df, df["_lookup_key"] == mapping_df["_match_key"], "left")

    return df.withColumn(output_col, F.col(f"_{output_col}_mapped")).drop("_lookup_key", "_match_key", f"_{output_col}_mapped")


def _first_regex_match(col_expr, rules: list) -> F.Column:
    """First-match-wins regex mapper."""
    out = F.lit(None).cast("string")
    for pattern, target in reversed(rules):
        out = F.when(col_expr.rlike(pattern), F.lit(target)).otherwise(out)
    return out



def _clean_brand_noise_values() -> list:
    """Python-side cleanup for BRAND_NOISE_VALUES. Cached safely."""
    out = set()
    for x in BRAND_NOISE_VALUES:
        x = html.unescape(str(x)).lower().strip()
        x = re.sub(r"[/_,|\-]+", " ", x)
        x = re.sub(r"\s*&\s*", " & ", x)
        x = re.sub(r"[^\w\s&']", " ", x)
        x = re.sub(r"\s+", " ", x).strip()

        if x and x not in NULL_LIKE_VALUES:
            out.add(x)

    return sorted(list(out))


def _first_token_from_name(name_col: str) -> F.Column:
    """Fallback brand from first meaningful word in product_name."""
    n = _normalize_text(name_col)
    token = F.regexp_extract(n, r"^([a-z0-9']{2,})\b", 1)

    return F.when(
        token.isNull() | (token == "") | token.isin(*NULL_LIKE_VALUES),
        F.lit(None).cast("string")
    ).otherwise(F.initcap(token))


def _get_spark_unit_map() -> F.Column:
    """Build Spark map for small dictionaries (like units) where Broadcast overhead isn't needed."""
    return F.create_map([F.lit(x) for item in UNIT_MAP.items() for x in item])


def _swap_fix(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        cur, old = _safe_double('product_current_price'), _safe_double('product_old_price')
        swapped = old.isNotNull() & (old < cur)

        fixed_cur = F.coalesce(F.when(swapped, old).otherwise(cur), F.lit(0.0))
        fixed_old = F.coalesce(F.when(swapped, cur).otherwise(old), fixed_cur, F.lit(0.0))

        return df.withColumns({'product_current_price': fixed_cur, 'product_old_price': fixed_old})
    return transform


def _noon_reconstruct(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        cur, old = _safe_double('product_current_price'), _safe_double('product_old_price')

        discount_col = F.lower(F.col('product_discount'))
        is_egp = discount_col.rlike('egp|pounds|pound|جنيه|جنيهات')
        is_pct = discount_col.contains('%')
        dv = _safe_double(F.regexp_extract('product_discount', r'(\d+(?:\.\d+)?)', 1))

        reconstructed_old = F.when(
            old.isNull() & dv.isNotNull(),
            F.when(is_egp, cur + dv).when(is_pct, cur / (1 - (dv / 100))).otherwise(old)
        ).otherwise(old)

        return df.withColumns({
            'product_current_price': F.coalesce(cur, F.lit(0.0)),
            'product_old_price': F.coalesce(reconstructed_old, cur, F.lit(0.0))
        })
    return transform


def _talabat_reconstruct(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    def transform(df: SparkDataFrame) -> SparkDataFrame:
        cur = F.coalesce(_safe_double('product_current_price'), F.lit(0.0))
        old_raw = _safe_double('product_old_price')
        dv = _safe_double(F.regexp_extract(F.col('product_discount'), r'(\d+)', 1))

        calc_old = cur / (1 - (dv / 100))
        final_old = F.when(old_raw.isNull() & dv.isNotNull(), calc_old).otherwise(F.coalesce(old_raw, cur))

        return df.withColumns({'product_current_price': cur, 'product_old_price': final_old})
    return transform