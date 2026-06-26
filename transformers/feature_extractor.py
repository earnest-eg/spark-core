from typing import Callable

from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from utils.decorators import safe_step 
from transformers.constants import (
    UNIT_REGEX,
    CATEGORY_EXACT_MAP,
    SUBCATEGORY_EXACT_MAP,
    CATEGORY_RULES,
    SUBCATEGORY_RULES
)

from transformers.helpers import (
    _clean,
    _safe_double,
    _swap_fix,
    _noon_reconstruct,
    _talabat_reconstruct,
    _get_spark_unit_map,
    _normalize_text,
    _first_regex_match,
    _clean_brand_noise_values,
    _first_token_from_name,
    apply_broadcast_mapping
)



@safe_step("Normalize Prices")
def normalize_prices(config: dict) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Normalizes price columns based on the specified strategy in the configuration.

    Args:
        config (dict): Configuration dictionary containing the 'price_strategy' key which determines the normalization strategy to apply. 
            Supported strategies include 'swap_fix', 'noon_reconstruct', and 'talabat_reconstruct'.   

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function that normalizes price columns according to the specified strategy.

    Raises:
        SchemaMismatchError: If required price or discount columns are missing during transformation.
        ComputationError: If a calculation error occurs while reconstructing prices.
    
    Output Schema:
        - product_current_price: double
        - product_old_price: double
        - product_discount_amount: double
        - product_discount_percentage: double
        - product_has_discount: boolean
    
    Process Description:
        1. Normalizes price columns based on the specified strategy in the configuration.
        2. Calculates discount amount and percentage.
        3. Calculates product has discount.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'price_strategy' key which determines the normalization strategy to apply. 
            Supported strategies include 'swap_fix', 'noon_reconstruct', and 'talabat_reconstruct'.        
    """

    _STRATEGIES = {
        'swap_fix': _swap_fix,
        'noon_reconstruct': _noon_reconstruct,
        'talabat_reconstruct': _talabat_reconstruct
    }

    strategy_name = config.get('price_strategy')
    strategy_func = _STRATEGIES.get(str(strategy_name))

    return strategy_func(config) if strategy_func else lambda df: df


@safe_step("Calculate Discounts")
def calculate_discount(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Calculates discount amount and percentage based on current and old price columns.

    Args:
        config (dict, optional): Configuration dictionary (Optional), for any future updates. Defaults to None.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function that calculates discount-related columns.

    Raises:
        SchemaMismatchError: If required price or discount columns are missing during transformation.
        ComputationError: If a calculation error occurs while calculating discounts.

    Output Schema:
        - product_discount_amount: double
        - product_discount_percentage: double
        - product_has_discount: boolean
    
    Process Description:
        1. Calculates discount amount and percentage.
        2. Calculates product has discount.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
    """

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        amount_discount = F.when(
            F.col('product_old_price') > F.col('product_current_price'),
            F.col('product_old_price') - F.col('product_current_price')
        ).otherwise(F.lit(0.0))

        percent_discount = F.when(
            F.col('product_old_price') > F.col('product_current_price'),
            (amount_discount / F.col('product_old_price')) * 100
        ).otherwise(F.lit(0.0))

        return df.withColumns({
            'product_discount_amount'     : amount_discount,
            'product_discount_percentage' : percent_discount,
            'product_has_discount'        : amount_discount > 0
        })

    return transform


@safe_step("Normalize Availability")
def normalize_availability(config: dict) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Normalizes the product availability status based on the specified strategy in the configuration.

    Args:
        config (dict): Configuration dictionary containing the 'availability' key which determines the normalization strategy to apply. 
            Supported strategies include 'stock_string' which checks for stock-related keywords in the availability column, 
            and 'price_positive' which considers products with a positive current price as available.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function that normalizes the product availability status.

    Raises:
        SchemaMismatchError: If the required availability column is missing during transformation.
        ComputationError: If a calculation error occurs while normalizing availability.

    Output Schema:
        - product_availability: boolean
    
    Process Description:
        1. Normalizes the product availability status based on the specified strategy in the configuration.
        2. Uses 'stock_string' strategy to check for stock-related keywords in the availability column.
        3. Uses 'price_positive' strategy to consider products with a positive current price as available.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'availability' key which determines the normalization strategy to apply. 
            Supported strategies include 'stock_string', and 'price_positive'.
    """

    strategy = config.get('availability', 'price_fillna')

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        avail_col = F.col('product_availability')
        price_positive = (F.col('product_current_price') > 0)

        available_cond = {
            'stock_string': F.coalesce(F.lower(avail_col).rlike('in_stock|true|1|available'), F.lit(False)),
            'price_positive': price_positive
        }.get(strategy, F.coalesce(avail_col.cast('boolean'), price_positive))

        return df.withColumn('product_availability', available_cond)

    return transform


@safe_step("Normalize Categories")
def normalize_categories(config: dict) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Normalize product category/subcategory using deterministic rules only.

    Important: this stage intentionally leaves unresolved values as NULL so
    fill_na_with_llm() can enrich them later. Final defaults belong after LLM.

    Args:
        config (dict, optional): Configuration dictionary containing the 'category' key which determines the category strategy to apply. 
            Supported strategies include 'swap_fix', 'noon_reconstruct', and 'talabat_reconstruct'.

    Output Schema:
        - product_category: string
        - product_subcategory: string
        - product_brand: string
    
    Process Description:
        1. Normalizes product category/subcategory using deterministic rules only.
        2. Leaves unresolved values as NULL so fill_na_with_llm() can enrich them later.
        3. Applies exact mapping rules for categories and subcategories.
        4. Applies category rules to extract category and subcategory from product name.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'category' key which determines the category strategy to apply. 
            Supported strategies include 'swap_fix', 'noon_reconstruct', and 'talabat_reconstruct'.
    """
    config = config or {}

    cat_col   = config.get("category_col", "product_category")
    sub_col   = config.get("subcategory_col", "product_subcategory")
    name_col  = config.get("name_col", "product_name")
    brand_col = config.get("brand_col", "product_brand")

    fill_defaults = config.get("fill_category_defaults_before_llm", False)
    unk_cat = config.get("unknown_category", "unknown")
    unk_sub = config.get("unknown_subcategory", "unknown")

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        for c in [cat_col, sub_col, name_col, brand_col]:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))

        df = df.withColumns({
            "cat_norm": _normalize_text(cat_col),
            "sub_norm": _normalize_text(sub_col),
            "name_norm": _normalize_text(name_col)
        })

        brand_noise = _clean_brand_noise_values()
        df = df.withColumns({
            "cat_signal": F.when(F.col("cat_norm").isin(*brand_noise), F.lit(None)).otherwise(F.col("cat_norm")),
            "sub_signal": F.when(F.col("sub_norm").isin(*brand_noise), F.lit(None)).otherwise(F.col("sub_norm"))
        })

        df = apply_broadcast_mapping(df, CATEGORY_EXACT_MAP, "cat_signal", "cat_exact")
        df = apply_broadcast_mapping(df, CATEGORY_EXACT_MAP, "sub_signal", "sub_as_cat_exact")

        df = apply_broadcast_mapping(df, SUBCATEGORY_EXACT_MAP, "sub_signal", "sub_exact")
        df = apply_broadcast_mapping(df, SUBCATEGORY_EXACT_MAP, "cat_signal", "cat_as_sub_exact")

        final_category = F.coalesce(
            F.col("cat_exact"),
            _first_regex_match(F.col("cat_signal"), CATEGORY_RULES),
            F.col("sub_as_cat_exact"),
            _first_regex_match(F.col("sub_signal"), CATEGORY_RULES),
            _first_regex_match(F.col("name_norm"), CATEGORY_RULES)
        )

        final_subcategory = F.coalesce(
            F.col("sub_exact"),
            _first_regex_match(F.col("sub_signal"), SUBCATEGORY_RULES),
            F.col("cat_as_sub_exact"),
            _first_regex_match(F.col("cat_signal"), SUBCATEGORY_RULES),
            _first_regex_match(F.col("name_norm"), SUBCATEGORY_RULES)
        )

        if fill_defaults:
            final_category = F.coalesce(final_category, F.lit(unk_cat))
            final_subcategory = F.coalesce(final_subcategory, F.lit(unk_sub))

        df = df.withColumns({
            "product_category": final_category,
            "product_subcategory": final_subcategory
        })

        temp_cols = [
            "cat_norm", "sub_norm", "name_norm", "cat_signal", "sub_signal",
            "cat_exact", "sub_as_cat_exact", "sub_exact", "cat_as_sub_exact"
        ]
        return df.drop(*temp_cols)

    return transform


@safe_step("Extract Units, Weight, and Count")
def extract_units_weight_count(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Extracts and normalizes product measuring unit, weight, and count from the product name or weight columns based on the specified configuration.
    
    Args:
        config (dict, optional): Configuration dictionary containing keys for input and output column names, as well as a default unit. 
            Defaults to None, in which case default column names and 'Piece' as the default unit will be used.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function that extracts and normalizes product measuring unit, weight, and count.

    Raises:
        SchemaMismatchError: If required input columns are missing during transformation.
        ComputationError: If a calculation error occurs while extracting and normalizing units, weight, or count.

    Output Schema:
        - product_measuring_unit: string
        - product_count: int
        - product_weight: string
    
    Process Description:
        1. Extracts and normalizes product measuring unit, weight, and count from the product name or weight columns based on the specified configuration.
        2. Uses regular expressions to extract units, weight, and count from the product name or weight column.
        3. Normalizes the extracted units, weight, and count to a standard format.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'input' key which determines the input column names, and the 'output' key which determines the output column names.
    """

    config = config or {}

    input_weight  = config.get("input_weight_col", "product_weight")
    input_name    = config.get("input_name_col",   "product_name")
    output_unit   = config.get("output_unit_col",  "product_measuring_unit")
    output_count  = config.get("output_count_col", "product_count")
    output_weight = config.get("output_weight_col","product_weight")
    default_unit  = config.get("default_unit",     "Piece")

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        cols = df.columns
        weight_expr = F.col(input_weight) if input_weight in cols else F.lit(None).cast('string')
        name_expr = F.col(input_name) if input_name in cols else F.lit(None).cast('string')

        raw      = F.coalesce(_clean(weight_expr), _clean(F.regexp_extract(name_expr, UNIT_REGEX, 1)))
        raw_unit = F.lower(_clean(F.regexp_extract(raw, r'([A-Za-z]+)', 1)))

        unit = F.initcap(F.coalesce(_get_spark_unit_map().getItem(raw_unit), raw_unit, F.lit(default_unit)))

        num = _safe_double(F.regexp_extract(raw, r'(\d+(?:\.\d+)?)', 1))
        is_piece = unit.eqNullSafe(F.lit('Piece'))

        return df.withColumns({
            output_unit: unit,
            output_count: F.when(is_piece, F.coalesce(num, F.lit(1.0))).otherwise(F.lit(1.0)),
            output_weight: F.when(is_piece, F.lit(0.0)).otherwise(F.coalesce(num, F.lit(0.0))),
        })

    return transform


@safe_step("Extract Brand")
def extract_brand(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Extract brand from trusted rule-based signals only.

    Important: unresolved values stay NULL so fill_na_with_llm() can enrich
    product_brand. Use brand_strategy="title_only" only for sellers where the
    first token of product_name is a reliable brand signal.

    Args:
        config (dict, optional): Configuration dictionary containing the 'brand' key which determines the brand strategy to apply. 
            Supported strategies include 'swap_fix', 'noon_reconstruct', and 'talabat_reconstruct'.

    Output Schema:
        - product_brand: string
    
    Process Description:
        1. Extracts and normalizes brand from the product name or weight column based on the specified configuration.
        2. Uses regular expressions to extract brand from the product name or weight column.
        3. Normalizes the extracted brand to a standard format.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
        - Configuration dictionary containing the 'brand' key which determines the brand strategy to apply. 
            Supported strategies include 'swap_fix', 'noon_reconstruct', and 'talabat_reconstruct'.
    """
    config    = config or {}

    brand_col = config.get("brand_col", "product_brand")
    name_col  = config.get("name_col", "product_name")
    cat_col   = config.get("category_col", "product_category")
    sub_col   = config.get("subcategory_col", "product_subcategory")
    strategy  = config.get("brand_strategy", "combine_first")

    use_name_first_token = strategy in {"title_only", "first_token", "name_first_token"}
    fill_default = config.get("fill_brand_default_before_llm", False)
    default_brand = config.get("default_brand", "unbranded")

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        for c in [brand_col, name_col, cat_col, sub_col]:
            if c not in df.columns:
                df = df.withColumn(c, F.lit(None).cast("string"))

        brand_norm = _normalize_text(brand_col)
        cat_norm = _normalize_text(cat_col)
        sub_norm = _normalize_text(sub_col)

        brand_noise = _clean_brand_noise_values()

        brand_from_col = F.when(brand_norm.isNotNull(), F.initcap(brand_norm))
        cat_as_brand = F.when(cat_norm.isin(*brand_noise), F.initcap(cat_norm))
        sub_as_brand = F.when(sub_norm.isin(*brand_noise), F.initcap(sub_norm))

        brand_from_name = _first_token_from_name(name_col) if use_name_first_token else F.lit(None).cast("string")

        final_brand = F.coalesce(brand_from_col, cat_as_brand, sub_as_brand, brand_from_name)

        if fill_default:
            final_brand = F.coalesce(final_brand, F.lit(default_brand))

        brand_source = (
            F.when(brand_from_col.isNotNull(), F.lit("brand_column"))
            .when(cat_as_brand.isNotNull(), F.lit("category_brand_noise"))
            .when(sub_as_brand.isNotNull(), F.lit("subcategory_brand_noise"))
            .when(brand_from_name.isNotNull(), F.lit("product_name_first_token"))
            .otherwise(F.lit("llm_pending"))
        )

        return df.withColumns({
            "product_brand": final_brand,
            "product_brand_source": brand_source
        })

    return transform


def tag_talabat_seller(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Tags products sold by Talabat by creating a new boolean column 'product_is_talabat_seller' based on the 'seller' column.

    Args:
        config (dict, optional): Configuration dictionary (Optional), for any future updates. Defaults to None.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A Spark transformation function that tags Talabat sellers.

    Raises:
        SchemaMismatchError: If the required 'seller' column is missing during transformation.
        ComputationError: If a calculation error occurs while tagging Talabat sellers.

    Output Schema:
        - product_is_talabat_seller: boolean
    
    Process Description:
        1. Tags products sold by Talabat by creating a new boolean column 'product_is_talabat_seller' based on the 'seller' column.
        2. Uses 'product_seller' column to determine if the product is sold by Talabat.
        3. Uses '==' operator to compare 'product_seller' column with 'talabat'.
    
    Requires:
        - SparkSession
        - PySpark DataFrame
    """

    config = config or {}

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        return df.withColumn('product_is_talabat_seller', F.col('product_seller') == 'talabat')

    return transform