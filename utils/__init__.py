"""
This package exposes utility modules for the project.

It provides lazy import functionality for utility attributes, allowing
for dynamic loading of utility values without requiring all modules
to be imported at once.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any, Final


if TYPE_CHECKING:
    from .decorators import safe_step
    from .decorators import handle_agent_errors

__version__: Final[str] = "1.0.0"


_EXPORTS: Final[dict[str, tuple[str, str]]] = {
    "safe_step": ("utils.decorators", "safe_step"),
    "handle_agent_errors": ("utils.decorators", "handle_agent_errors"),
}

__all__: Final[list[str]] = list(_EXPORTS.keys())



def __getattr__(name: str) -> Any:
    """
    Lazy import utility functions/decorators with memoization.
    """
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_path, attr = _EXPORTS[name]

    try:
        module = import_module(module_path)
        value = getattr(module, attr)
    except ImportError as exc:
        raise ImportError(
            f"Failed loading utility '{name}' from '{module_path}'. "
            f"Check package layout and dependencies."
        ) from exc


    globals()[name] = value
    return value



def __dir__() -> list[str]:
    return sorted(list(globals().keys()) + __all__)