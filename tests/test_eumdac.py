import pytest

from tests.generic import *
from sand.sample_product import products
from sand.eumdac import DownloadEumDAC


@pytest.fixture(params=['SENTINEL-3-OLCI-FR'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param

@pytest.fixture
def constraint(collec):
    return products[collec]['level1']


def test_login(collec, level):
    eval_login(DownloadEumDAC, collec, level)

def test_collection():
    eval_collection(DownloadEumDAC)

def test_download(collec, level, constraint):
    eval_download(DownloadEumDAC, collec, level, **constraint)

def test_metadata(collec, level, constraint):
    eval_metadata(DownloadEumDAC, collec, level, **constraint)
    
def test_quicklook(request, collec, level, constraint):
    eval_quicklook(request, DownloadEumDAC, collec, level, **constraint)
    