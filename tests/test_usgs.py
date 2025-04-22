import pytest

from tests.generic import *
from sand.sample_product import products
from sand.usgs import DownloadUSGS


@pytest.fixture(params=['LANDSAT-5-TM'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param

@pytest.fixture
def constraint(collec):
    return products[collec]['level1']


def test_login(collec, level):
    eval_login(DownloadUSGS, collec, level)

def test_collection():
    eval_collection(DownloadUSGS)

def test_download(collec, level, constraint):
    eval_download(DownloadUSGS, collec, level, **constraint)
    
def test_metadata(collec, level, constraint):
    eval_metadata(DownloadUSGS, collec, level, **constraint)
    
def test_quicklook(request, collec, level, constraint):
    eval_quicklook(request, DownloadUSGS, collec, level, **constraint)