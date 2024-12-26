import pytest

from tests.generic import *
from sand.copernicus_dataspace import DownloadCDSE
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['SENTINEL-2-MSI'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    

def test_login(collec, level):
    eval_login(DownloadCDSE, collec, level)

def test_collection():
    eval_collection(DownloadCDSE)

def test_download(collec, level):
    eval_download(DownloadCDSE, collec, level,
                  dtstart = datetime(2024, 1, 1),
                  dtend = datetime(2024, 2, 1),
                  geo = Point(119.514442, -8.411750),
                  name_contains = ['_MSIL1C_'])

def test_metadata(collec, level):
    eval_metadata(DownloadCDSE, collec, level,
                  dtstart = datetime(2024, 1, 1),
                  dtend = datetime(2024, 2, 1),
                  geo = Point(119.514442, -8.411750),
                  name_contains = ['_MSIL1C_'])
    
def test_quicklook(request, collec, level):
    eval_quicklook(request, DownloadCDSE, collec, level,
                   dtstart = datetime(2024, 1, 1),
                   dtend = datetime(2024, 2, 1),
                   geo = Point(119.514442, -8.411750),
                   name_contains = ['_MSIL1C_'])
    