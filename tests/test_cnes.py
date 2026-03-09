import pytest

from tests.generic import *
from sand.sample_product import products
from sand.cnes import DownloadCNES


@pytest.fixture
def downloader():
    return DownloadCNES()

@pytest.fixture(params=['VENUS'])
def collec(request): return request.param
    
@pytest.fixture(params=[1])
def level(request): return request.param
    
@pytest.fixture
def constraint(collec): return products[collec]['constraint']

@pytest.fixture
def product_id(): return products['SENTINEL-1-SAR']['l1_product']


def test_login(downloader):
    eval_login(downloader)

def test_collection(downloader):
    eval_collection(downloader)

def test_download(downloader, collec, level, constraint):
    eval_download(downloader, collec, level, **constraint)

def test_metadata(downloader, collec, level, constraint):
    eval_metadata(downloader, collec, level, **constraint)

def test_quicklook(request, downloader, collec, level, constraint):
    eval_quicklook(request, downloader, collec, level, **constraint)
            
def test_download_file(downloader, product_id):
    eval_download_file(downloader, product_id)

def test_venus_download_file():
    with TemporaryDirectory() as tmpdir:
        dl = DownloadCNES()
        prod_id = products['VENUS']['l1_product']
        out = dl.download_file(prod_id, tmpdir)
        assert out.exists()
        assert out.name.startswith(prod_id)