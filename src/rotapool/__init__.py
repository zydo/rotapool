from importlib.metadata import version as _version

from .exceptions import CooldownResource, DisableResource, PoolExhausted
from .models import Resource
from .pool import Pool

__version__ = _version("rotapool")
__all__ = [
    "CooldownResource",
    "DisableResource",
    "Pool",
    "PoolExhausted",
    "Resource",
    "__version__",
]
