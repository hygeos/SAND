import pytest

from tests.generic import *
from sand.sample_product import products
from sand.theia import DownloadTHEIA


@pytest.fixture(params=['VENUS'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    
@pytest.fixture
def constraint(collec):
    return products[collec]['level1']


def test_login(collec, level):
    eval_login(DownloadTHEIA, collec, level)

def test_collection():
    eval_collection(DownloadTHEIA)

def test_download(collec, level, constraint):
    eval_download(DownloadTHEIA, collec, level, **constraint)

def test_metadata(collec, level, constraint):
    with pytest.raises(FileNotFoundError):
        eval_metadata(DownloadTHEIA, collec, level, **constraint)
    
def test_quicklook(request, collec, level, constraint):
    eval_quicklook(request, DownloadTHEIA, collec, level, **constraint)
    