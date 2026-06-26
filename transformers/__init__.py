"""
This package exposes transformer modules for the project.

It provides lazy import functionality for transformer attributes, allowing
for dynamic loading of transformer values without requiring all modules
to be imported at once.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from transformers.cleaner import (
        drop_and_rename, fill_nulls, drop_temps, 
        remove_duplicate, cast_data_types, drop_unneeded_columns
    )
    from transformers.date_time_extractor import add_date, add_time
    from transformers.feature_extractor import (
        normalize_prices, calculate_discount, normalize_availability,
        normalize_categories, extract_units_weight_count, 
        extract_brand, tag_talabat_seller
    )
    from transformers.llm_imputer import fill_na_with_llm


__version__: Final[str] = "1.0.0"


_EXPORTS: Final[dict[str, tuple[str, str]]] = {
    "drop_and_rename": ("transformers.cleaner", "drop_and_rename"),
    "fill_nulls": ("transformers.cleaner", "fill_nulls"),
    "drop_temps": ("transformers.cleaner", "drop_temps"),
    "remove_duplicate": ("transformers.cleaner", "remove_duplicate"),
    "cast_data_types": ("transformers.cleaner", "cast_data_types"),
    "drop_unneeded_columns": ("transformers.cleaner", "drop_unneeded_columns"),
    
    "add_date": ("transformers.date_time_extractor", "add_date"),
    "add_time": ("transformers.date_time_extractor", "add_time"),

    "normalize_prices": ("transformers.feature_extractor", "normalize_prices"),
    "calculate_discount": ("transformers.feature_extractor", "calculate_discount"),
    "normalize_availability": ("transformers.feature_extractor", "normalize_availability"),
    "normalize_categories": ("transformers.feature_extractor", "normalize_categories"),
    "extract_units_weight_count": ("transformers.feature_extractor", "extract_units_weight_count"),
    "extract_brand": ("transformers.feature_extractor", "extract_brand"),
    "tag_talabat_seller": ("transformers.feature_extractor", "tag_talabat_seller"),

    "fill_na_with_llm": ("transformers.llm_imputer", "fill_na_with_llm"),
}

__all__: Final[list[str]] = list(_EXPORTS.keys())


def __getattr__(name: str) -> Any:
    """
    Lazy import transformer functions with caching.
    """
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_path, attr = _EXPORTS[name]

    try:
        module = import_module(module_path)
        value = getattr(module, attr)
    except ImportError as exc:
        raise ImportError(
            f"Failed loading transformer '{name}' from '{module_path}'. "
            f"Check package dependencies."
        ) from exc

    globals()[name] = value
    return value
def __dir__() -> list[str]:
    """
    Makes dynamic environments (like Jupyter notebooks or REPLs) aware of the lazy imports.
    """
    return sorted(list(globals().keys()) + __all__)