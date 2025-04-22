import pytest

from tests.generic import *
from sand.sample_product import products
from sand.nasa import DownloadNASA


@pytest.fixture(params=['ECOSTRESS'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    
@pytest.fixture
def constraint(collec):
    return products[collec]['level1']


def test_login(collec, level):
    eval_login(DownloadNASA, collec, level)

def test_collection():
    eval_collection(DownloadNASA)

def test_download(collec, level, constraint):
    eval_download(DownloadNASA, collec, level, **constraint)

def test_metadata(collec, level, constraint):
    eval_metadata(DownloadNASA, collec, level, **constraint)

def test_quicklook(request, collec, level, constraint):
    eval_quicklook(request, DownloadNASA, collec, level, **constraint)
    