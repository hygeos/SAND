import pytest

from tests.generic import *
from sample_product import products
from sand.copernicus_dataspace import DownloadCDSE


@pytest.fixture(params=['SENTINEL-2-MSI'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    
@pytest.fixture
def constraint(collec):
    return products[collec]['level1']

def test_login(collec, level):
    eval_login(DownloadCDSE, collec, level)

def test_collection():
    eval_collection(DownloadCDSE)

def test_download(collec, level, constraint):
    eval_download(DownloadCDSE, collec, level, **constraint)

def test_metadata(collec, level, constraint):
    eval_metadata(DownloadCDSE, collec, level, **constraint)
    
def test_quicklook(request, collec, level, constraint):
    eval_quicklook(request, DownloadCDSE, collec, level, **constraint)
    