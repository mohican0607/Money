"""Compatibility shim — prefer ``src.learning.market_theme``."""
from src.learning import market_theme as _mt

# ``import *`` 는 ``_`` 이름을보내지 않으므로 진단 스크립트 호환을 위해 전부 복사.
for _name, _val in vars(_mt).items():
    if _name.startswith("__"):
        continue
    globals()[_name] = _val

del _mt, _name, _val
