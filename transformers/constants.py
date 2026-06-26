"""
Lightweight constants module.

Static constants (UNIT_REGEX, UNIT_MAP, NULL_LIKE_VALUES) live here.
Category / subcategory / brand data is loaded lazily from category_registry.json
via module-level __getattr__, so existing imports like:

    from transformers.constants import CATEGORY_EXACT_MAP

continue to work unchanged.
"""

import json
import os
import shutil
from typing import Final


UNIT_REGEX: Final[str] = (
    r'(?i)(\d+(?:\.\d+)?(?:\s*|)'
    r'(?:kg|kilogram|kilograms|g|gram|grams|gm|m|ml|milliliter|l|liter|liters?|pcs?|piece)'
    r'(?:\b|$))'
)

UNIT_MAP: Final[dict[str, str]] = {
    'kg': 'Kilogram', 'kilogram': 'Kilogram', 'kilograms': 'Kilogram',
    'g': 'Gram', 'gram': 'Gram', 'grams': 'Gram', 'gm': 'Gram',
    'm': 'Milliliter', 'ml': 'Milliliter', 'milliliter': 'Milliliter',
    'l': 'Liter', 'liter': 'Liter', 'liters': 'Liter',
    'pc': 'Piece', 'pcs': 'Piece', 'piece': 'Piece',
}

NULL_LIKE_VALUES: Final[list[str]] = [
    "", " ", "nan", "none", "null", "na", "n/a", "n\\a",
    "undefined", "unknown", "not available", "n.a", "-", "--"
]

STRING_COLS: Final[list[str]] = [
    'product_name', 'product_url', 'product_category', 'product_subcategory',
    'product_measuring_unit', 'product_brand', 'product_seller',
    'timestamp_timezone', 'product_image_url', 'product_ram', 'product_storage',
    'holyday_name'
]

BOOL_COLS: Final[list[str]] = [
    'product_has_discount', 'product_has_image_url', 'product_has_ram',
    'product_has_storage', 'product_availability', 'product_is_talabat_seller',
    'is_holyday',
]

TEMP_COLS: Final[frozenset[str]] = frozenset([
    'product_discount', 'temp_current', 'temp_old', 'temp_extracted_unit',
    'temp_extracted_num', 'temp_ext', 'temp_num', 'reconstructed_old',
    'is_egp_discount', 'is_pct_discount', 'discount_val', 'temp_discount_val',
])

_REGISTRY_PATH: Final[str] = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "category_registry.json"
)

_TARGETS = ["product_category", "product_subcategory", "product_brand"]

DEFAULT_CACHE_PATH = "Data/parquet_cache"

_registry_cache: dict | None = None

_LAZY_ATTRS: Final[dict[str, str]] = {
    "CATEGORY_EXACT_MAP":  "category_aliases",
    "SUBCATEGORY_EXACT_MAP": "subcategory_aliases",
    "CATEGORY_RULES":      "category_rules",
    "SUBCATEGORY_RULES":   "subcategory_rules",
    "BRAND_NOISE_VALUES":  "brand_noise_values",
    "CANONICAL_BRANDS":    "canonical_brands",
    "BRAND_ALIASES":       "brand_aliases",
}


def load_registry() -> dict:
    """Load the category registry from JSON (cached after first call)."""
    global _registry_cache
    if _registry_cache is None:
        with open(_REGISTRY_PATH, "r", encoding="utf-8") as f:
            _registry_cache = json.load(f)
    return _registry_cache


def save_registry(data: dict) -> None:
    """Atomically persist the registry back to JSON."""
    global _registry_cache
    tmp = _REGISTRY_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    shutil.move(tmp, _REGISTRY_PATH)
    _registry_cache = data


def reload_registry() -> dict:
    """Force-reload the registry from disk (invalidates cache)."""
    global _registry_cache
    _registry_cache = None
    return load_registry()


def __getattr__(name: str):
    """Module-level lazy loader for registry-backed constants."""
    key = _LAZY_ATTRS.get(name)
    if key is not None:
        reg = load_registry()
        value = reg[key]
        if name in ("CATEGORY_RULES", "SUBCATEGORY_RULES"):
            value = [(p, t) for p, t in value]
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")