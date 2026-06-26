"""
This package exposes error modules for the project.

It provides lazy import functionality for error attributes, allowing
for dynamic loading of error values without requiring all modules
to be imported at once.
"""


from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from .AgentBaseError import AgentBaseError
    from .AgentRateLimitError import AgentRateLimitError
    from .ConfigurationError import ConfigurationError
    from .ComputationError import ComputationError
    from .DataQualityError import DataQualityError
    from .PipelineBaseError import PipelineBaseError
    from .SchemaMismatchError import SchemaMismatchError
    from .AgentAPIError import AgentAPIError

__version__: Final[str] = "1.0.0"

_EXPORTS: Final[dict[str, tuple[str, str]]] = {
    "AgentBaseError": ("errors.AgentBaseError", "AgentBaseError"),
    "AgentAPIError": ("errors.AgentAPIError", "AgentAPIError"),
    "AgentRateLimitError": ("errors.AgentRateLimitError", "AgentRateLimitError"),
    "ConfigurationError": ("errors.ConfigurationError", "ConfigurationError"),
    "ComputationError": ("errors.ComputationError", "ComputationError"),
    "DataQualityError": ("errors.DataQualityError", "DataQualityError"),
    "PipelineBaseError": ("errors.PipelineBaseError", "PipelineBaseError"),
    "SchemaMismatchError": ("errors.SchemaMismatchError", "SchemaMismatchError"),
}

__all__: Final[list[str]] = list(_EXPORTS.keys())


def __getattr__(name: str) -> Any:
    """
    Lazy import error classes with memoization.
    """
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_path, attr = _EXPORTS[name]

    try:
        module = import_module(module_path)
        value = getattr(module, attr)
    except ImportError as exc:
        raise ImportError(
            f"Failed loading error class '{name}' from '{module_path}'. "
            f"Check package layout and dependencies."
        ) from exc

    globals()[name] = value
    return value
def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)