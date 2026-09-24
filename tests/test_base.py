"""
Offline unit tests for sand.base.

These tests require no network access or .netrc credentials. They cover:
  - check_too_many_matches (inverted-condition fix)
  - download_all (serial, parallelized, SandQuery input, empty input)
  - _get_with_redirects (redirect handling with a mocked session)
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sand.base import BaseDownload, check_too_many_matches
from sand.results import SandProduct, SandQuery


# ---------------------------------------------------------------------------
# Item 1: check_too_many_matches
# ---------------------------------------------------------------------------


def test_check_too_many_matches_warns_when_matches_exceed_returned(monkeypatch):
    """Warn when the provider reports more matches than it actually returned."""
    calls = []
    monkeypatch.setattr("sand.base.log.warning", lambda *a, **k: calls.append(a))

    response = {"context": {"returned": 10, "matched": 25}}
    check_too_many_matches(response, ["context", "returned"], ["context", "matched"])

    assert len(calls) == 1


def test_check_too_many_matches_no_warning_when_within_limit(monkeypatch):
    """No warning when returned >= matches (both the equal and greater cases)."""
    calls = []
    monkeypatch.setattr("sand.base.log.warning", lambda *a, **k: calls.append(a))

    check_too_many_matches(
        {"context": {"returned": 25, "matched": 10}},
        ["context", "returned"],
        ["context", "matched"],
    )
    check_too_many_matches(
        {"context": {"returned": 10, "matched": 10}},
        ["context", "returned"],
        ["context", "matched"],
    )

    assert len(calls) == 0


# ---------------------------------------------------------------------------
# Item 2: download_all (serial / parallelized / empty)
# ---------------------------------------------------------------------------


class DummyDownloader(BaseDownload):
    """Offline stand-in: 'download' just writes a marker file, no network."""

    provider = "dummy"
    nb_worker = 2

    def _login(self) -> None:
        # No real login for the dummy downloader.
        pass

    def download(self, product, dir, if_exists="skip"):
        out = Path(dir) / f"{product.product_id}.txt"
        out.write_text(product.product_id)
        return out


def _make_products(n):
    return [
        SandProduct(
            product_id=f"prod_{i}", date="2024-01-01", metadata={}, index=str(i)
        )
        for i in range(n)
    ]


def test_download_all_serial(tmp_path):
    dl = DummyDownloader(verbose=False)
    products = _make_products(3)
    out = dl.download_all(products, tmp_path)

    assert len(out) == 3
    assert all(Path(p).exists() for p in out)
    # Serial path preserves input order.
    assert [Path(p).name for p in out] == ["prod_0.txt", "prod_1.txt", "prod_2.txt"]


def test_download_all_accepts_sandquery(tmp_path):
    """download_all accepts a SandQuery, not just a plain list."""
    dl = DummyDownloader(verbose=False)
    query = SandQuery(_make_products(3))
    out = dl.download_all(query, tmp_path)

    assert len(out) == 3
    assert all(Path(p).exists() for p in out)


def test_download_all_parallelized(tmp_path):
    """Parallel path uses one downloader per worker process and preserves order."""
    dl = DummyDownloader(verbose=False)
    products = _make_products(4)
    out = dl.download_all(products, tmp_path, parallelized=True)

    # pool.map preserves input order.
    assert len(out) == 4
    assert [Path(p).name for p in out] == [
        "prod_0.txt",
        "prod_1.txt",
        "prod_2.txt",
        "prod_3.txt",
    ]
    assert all(Path(p).exists() for p in out)


def test_download_all_empty(tmp_path):
    """Empty input returns [] and does not spawn a pool (avoids Pool(0))."""
    dl = DummyDownloader(verbose=False)
    assert dl.download_all([], tmp_path) == []
    assert dl.download_all([], tmp_path, parallelized=True) == []


# ---------------------------------------------------------------------------
# Item 11: _get_with_redirects (mocked session)
# ---------------------------------------------------------------------------


def _resp(status_code, headers=None):
    r = MagicMock()
    r.status_code = status_code
    r.headers = headers or {}
    return r


def test_get_with_redirects_follows_chain():
    """A 302 -> 200 chain is followed and the final response is returned."""
    dl = DummyDownloader(verbose=False)
    dl.session = MagicMock()
    dl.session.get.side_effect = [
        _resp(302, {"Location": "http://host/b"}),
        _resp(200),
    ]

    final = dl._get_with_redirects("http://host/a")

    assert final.status_code == 200
    assert dl.session.get.call_count == 2
    # The default timeout is baked into the request.
    first_kwargs = dl.session.get.call_args_list[0][1]
    assert first_kwargs.get("timeout") == BaseDownload.TIMEOUT


def test_get_with_redirects_missing_location_raises():
    """A redirect without a Location header raises ValueError."""
    dl = DummyDownloader(verbose=False)
    dl.session = MagicMock()
    dl.session.get.side_effect = [_resp(302, {})]

    with pytest.raises(ValueError):
        dl._get_with_redirects("http://host/a")


def test_get_with_redirects_hop_limit():
    """Redirects stop after max_hops even if the chain is not finished."""
    dl = DummyDownloader(verbose=False)
    dl.session = MagicMock()
    dl.session.get.side_effect = [
        _resp(302, {"Location": "http://host/1"}),
        _resp(302, {"Location": "http://host/2"}),
        _resp(302, {"Location": "http://host/3"}),
    ]

    final = dl._get_with_redirects("http://host/0", max_hops=2)

    # 1 initial request + 2 hops = 3 calls, then it stops (still a 302).
    assert dl.session.get.call_count == 3
    assert final.status_code == 302


def test_get_with_redirects_non_redirect_passthrough():
    """A non-redirect response is returned as-is with a single request."""
    dl = DummyDownloader(verbose=False)
    dl.session = MagicMock()
    dl.session.get.side_effect = [_resp(200)]

    final = dl._get_with_redirects("http://host/a")

    assert final.status_code == 200
    assert dl.session.get.call_count == 1
