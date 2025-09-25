import pytest

from tests.generic import *
from sand.sample_product import products
from sand.usgs import DownloadUSGS


@pytest.fixture
def downloader():
    return DownloadUSGS()

@pytest.fixture(params=['LANDSAT-5-TM'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param

@pytest.fixture
def constraint(collec):
    return products[collec]['level1']
    
@pytest.fixture
def product_id(constraint): return constraint['product_id']


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