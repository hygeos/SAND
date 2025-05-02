import pytest

from tests.generic import *
from sand.sample_product import products
from sand.geodes import DownloadCNES


@pytest.fixture(params=['SENTINEL-1'])
def collec(request): return request.param
    
@pytest.fixture(params=[1])
def level(request): return request.param
    
@pytest.fixture
def constraint(collec): return products[collec]['level1']
    
@pytest.fixture
def product_id(collec): return products[collec]['level1']['product_id']


def test_login(collec, level):
    eval_login(DownloadCNES, collec, level)

def test_collection():
    eval_collection(DownloadCNES)

def test_download(collec, level, constraint):
    eval_download(DownloadCNES, collec, level, **constraint)

def test_metadata(collec, level, constraint):
    with pytest.raises(NotImplementedError):
        eval_metadata(DownloadCNES, collec, level, **constraint)
    
def test_quicklook(request, collec, level, constraint):
    eval_quicklook(request, DownloadCNES, collec, level, **constraint)
    
def test_download_file(product_id):
    eval_download_file(DownloadCNES, product_id)