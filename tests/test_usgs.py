import pytest

from tests.generic import *
from sand.usgs import DownloadUSGS
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['LANDSAT-5-TM'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param


def test_login(collec, level):
    eval_login(DownloadUSGS, collec, level)

def test_collection():
    eval_collection(DownloadUSGS)

def test_download(collec, level):
    eval_download(DownloadUSGS, collec, level,
                  dtstart = datetime(2000, 12, 10),
                  dtend = datetime(2005, 12, 10),
                  geo = Point(119.514442, -8.411750))
    
def test_metadata(collec, level):
    eval_metadata(DownloadUSGS, collec, level,
                  dtstart = datetime(2000, 12, 10),
                  dtend = datetime(2005, 12, 10),
                  geo = Point(119.514442, -8.411750))
    
def test_quicklook(request, collec, level):
    eval_quicklook(request, DownloadUSGS, collec, level,
                   dtstart = datetime(2000, 12, 10),
                   dtend = datetime(2005, 12, 10),
                   geo = Point(119.514442, -8.411750))