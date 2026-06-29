"""
This module contains configuration for the pipeline.

It provides default values for various pipeline parameters and allows for
specifying overrides for specific sellers.
"""

import os
from typing import Final

from transformers.constants import load_registry

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DATA_DIR = os.path.join(_BASE_DIR, "Data")


REGISTERED_SELLERS: Final[list[str]] = [
    "aggregated_beauty", "ariika", "bashrety", "btech", "cairosales",
    "carrefour", "chicco", "cleaned", "compumarts", "cstore",
    "decathlon", "dokkantech", "elghazawy", "ennap", "eva", "fresh",
    "gosport", "gourmet", "hyperone", "intersport", "jumia", "kimostore",
    "mahmoud_elfar", "mazaya", "meercato", "metro", "noon", "pipeline_final",
    "raya", "samir_and_aly", "seoudi", "sigma", "talabat",
    "the_beauty_secrets", "toptoys", "tradeline"
]


SELLER_FILES = {
    "aggregated_beauty"  : os.path.join(_DATA_DIR, "aggregated_beauty_products.csv"),
    "ariika"             : os.path.join(_DATA_DIR, "ariika_products_etl_clean_with_keys.csv"),
    "bashrety"           : os.path.join(_DATA_DIR, "bashrety_products_clean.csv"),
    "btech"              : os.path.join(_DATA_DIR, "btech.csv"),
    "cairosales"         : os.path.join(_DATA_DIR, "cairosales_products_clean.csv"),
    "carrefour"          : os.path.join(_DATA_DIR, "carrefour.csv"),
    "chicco"             : os.path.join(_DATA_DIR, "chicco_products.csv"),
    "cleaned"            : os.path.join(_DATA_DIR, "cleaned_products.csv"),
    "compumarts"         : os.path.join(_DATA_DIR, "compumarts_products.csv"),
    "cstore"             : os.path.join(_DATA_DIR, "cstore_cleaned_20260613_123142.csv"),
    "decathlon"          : os.path.join(_DATA_DIR, "decathlon_products_clean.csv"),
    "dokkantech"         : os.path.join(_DATA_DIR, "dokkantech.csv"),
    "elghazawy"          : os.path.join(_DATA_DIR, "elghazawy_products_cleaned.csv"),
    "ennap"              : os.path.join(_DATA_DIR, "ennap_products_clean.csv"),
    "eva"                : os.path.join(_DATA_DIR, "eva_products_clean.csv"),
    "fresh"              : os.path.join(_DATA_DIR, "fresh_products.csv"),
    "gosport"            : os.path.join(_DATA_DIR, "gosport_products_clean.csv"),
    "gourmet"            : os.path.join(_DATA_DIR, "gourmet.csv"),
    "hyperone"           : os.path.join(_DATA_DIR, "hyperone.csv"),
    "intersport"         : os.path.join(_DATA_DIR, "intersport_products_enriched.csv"),
    "jumia"              : os.path.join(_DATA_DIR, "jumia.csv"),
    "kimostore"          : os.path.join(_DATA_DIR, "kimostore_products.csv"),
    "mahmoud_elfar"      : os.path.join(_DATA_DIR, "mahmoud_elfar_products.csv"),
    "mazaya"             : os.path.join(_DATA_DIR, "mazaya_products_clean.csv"),
    "meercato"           : os.path.join(_DATA_DIR, "meercato_products_clean.csv"),
    "metro"              : os.path.join(_DATA_DIR, "metro.csv"),
    "noon"               : os.path.join(_DATA_DIR, "noon.csv"),
    "pipeline_final"     : os.path.join(_DATA_DIR, "pipeline_final_products.csv"),
    "raya"               : os.path.join(_DATA_DIR, "raya.csv"),
    "samir_and_aly"      : os.path.join(_DATA_DIR, "samir_and_aly_products_clean.csv"),
    "seoudi"             : os.path.join(_DATA_DIR, "seoudi.csv"),
    "sigma"              : os.path.join(_DATA_DIR, "sigma.csv"),
    "talabat"            : os.path.join(_DATA_DIR, "talabat.csv"),
    "the_beauty_secrets" : os.path.join(_DATA_DIR, "the_beauty_secrets_products.csv"),
    "toptoys"            : os.path.join(_DATA_DIR, "toptoys_products_clean.csv"),
    "tradeline"          : os.path.join(_DATA_DIR, "tradeline_products.csv")
}


_DEFAULTS = {
    "availability"            : "price_fillna",
    "scraping_utc"            : False,
    "default_category"        : "unspecified",
    "dedup_key"               : "product_url",
    "image_handling"          : "basic",
    "brand_strategy"          : "combine_first",
    "price_strategy"          : "swap_fix",
    "subcategory_strategy"    : "extract_from_category",
    "keep_csv_product_seller" : False,
}

_OVERRIDES = {
    "gourmet"   : {"image_handling": "placeholder"},
    "hyperone"  : {"availability": "stock_string", "scraping_utc": True}, 
    "jumia"     : {"scraping_utc": True},
    "metro"     : {"scraping_utc": True},
    "noon"      : {"scraping_utc": True, "price_strategy": "noon_reconstruct", "subcategory_strategy": "fillna_category"},
    "raya"      : {"dedup_key": "product_name"},
    "seoudi"    : {"availability": "stock_string", "scraping_utc": True},
    "sigma"     : {"availability": "price_positive", "default_category": "electronics", "image_handling": "unknown", "brand_strategy": "title_only"},
    "talabat"   : {"availability": "price_positive", "scraping_utc": True, "price_strategy": "talabat_reconstruct", "subcategory_strategy": "fillna_unspecified", "keep_csv_product_seller": True},
    "btech"     : {"availability": "price_positive", "default_category": "electronics"},
}


SELLER_CONFIGS = {
    seller: {**_DEFAULTS, **_OVERRIDES.get(seller, {})}
    for seller in REGISTERED_SELLERS
}



LLM_CACHE_PATH: Final[str] = os.getenv(
    "LLM_CACHE_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Data", "llm_cache"),
)


PROMPT_CACHE_PATH: Final[str] = os.getenv(
    "PROMPT_CACHE_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Data", "prompt_cache"),
)


LLM_BATCH_SIZE: Final[int] = int(os.getenv("LLM_BATCH_SIZE", "25"))


LLM_TARGET_COLS: Final[list[str]] = [
    "product_category",
    "product_subcategory",
    "product_brand",
]


DEFAULT_FALLBACK_VALUES: Final[dict[str, str]] = {
    "product_brand": "unbranded",
    "product_category": "unknown",
    "product_subcategory": "unknown",
    "product_seller": "unknown_seller",
    "product_measuring_unit": "unknown_unit",
    "product_ram": "unknown",
    "product_storage": "unknown",
    "product_name": "unknown_product",
    "product_count": "1",
}

_reg = load_registry()
VALID_CATEGORIES: Final[frozenset[str]] = frozenset(_reg["canonical_categories"])
VALID_SUBCATEGORIES: Final[frozenset[str]] = frozenset(_reg["canonical_subcategories"])

LOG_ROW_COUNTS: Final[bool] = os.getenv("LOG_ROW_COUNTS", "false").lower() in ("1", "true", "yes")
