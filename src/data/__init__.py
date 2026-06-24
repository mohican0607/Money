"""호환 shim — ``stocks``·``news``·``investor_flow`` 는 ``src/`` 루트로 이동."""
from .. import investor_flow, news, stocks

__all__ = ["stocks", "news", "investor_flow"]
