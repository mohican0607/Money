"""KRX 상장 목록 KIND 파싱·폐지 종목 제외."""
from __future__ import annotations

from src.stocks import _kind_corp_code, _parse_kind_corp_list_html


def test_kind_corp_code_normalizes_float_and_int() -> None:
    assert _kind_corp_code(5930) == "005930"
    assert _kind_corp_code(5930.0) == "005930"
    assert _kind_corp_code("5930.0") == "005930"
    assert _kind_corp_code("008500") == "008500"


def test_parse_kind_corp_list_keeps_kospi_kosdaq_drops_konex() -> None:
    html = """
    <table>
      <tr><th>회사명</th><th>시장구분</th><th>종목코드</th><th>업종</th></tr>
      <tr><td>삼성전자</td><td>유가</td><td>5930</td><td>전기전자</td></tr>
      <tr><td>삼익제약</td><td>코스닥</td><td>14950</td><td>제약</td></tr>
      <tr><td>코넥스더미</td><td>코넥스</td><td>123456</td><td>기타</td></tr>
    </table>
    """
    df = _parse_kind_corp_list_html(html)
    codes = set(df["Code"].astype(str))
    assert codes == {"005930", "014950"}
    assert df.loc[df["Code"] == "005930", "Market"].iloc[0] == "KOSPI"
    assert df.loc[df["Code"] == "014950", "Market"].iloc[0] == "KOSDAQ"


def test_parse_kind_corp_list_excludes_delisted_iljeong() -> None:
    html = """
    <table>
      <tr><th>회사명</th><th>시장구분</th><th>종목코드</th></tr>
      <tr><td>삼성전자</td><td>유가</td><td>005930</td></tr>
    </table>
    """
    df = _parse_kind_corp_list_html(html)
    assert "008500" not in set(df["Code"].astype(str))
    assert "일정실업" not in set(df["Name"].astype(str))
