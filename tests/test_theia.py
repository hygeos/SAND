import pytest

from tests.generic import *
from sand.theia import DownloadTHEIA
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['VENUS'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    


def test_login(collec, level):
    eval_login(DownloadTHEIA, collec, level)

def test_collection():
    eval_collection(DownloadTHEIA)

def test_download(collec, level):
    eval_download(DownloadTHEIA, collec, level,
                  dtstart = datetime(2020, 1, 1),
                  dtend = datetime(2020, 6, 1),
                  venus_site='NARYN')

def test_metadata(collec, level):
    with pytest.raises(AssertionError):
        eval_metadata(DownloadTHEIA, collec, level,
                    dtstart = datetime(2020, 1, 1),
                    dtend = datetime(2020, 6, 1),
                    venus_site='NARYN')
    
def test_quicklook(request, collec, level):
    eval_quicklook(request, DownloadTHEIA, collec, level,
                  dtstart = datetime(2020, 1, 1),
                  dtend = datetime(2020, 6, 1),
                  venus_site='NARYN')
    
    