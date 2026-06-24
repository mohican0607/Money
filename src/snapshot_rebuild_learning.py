"""Compatibility: ``from src.snapshot_rebuild_learning import _LIMIT_UP_RET_DECIMAL`` 등."""
from src.learning import market_theme as _mt

for _name, _val in vars(_mt).items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _val

del _mt, _name, _val
