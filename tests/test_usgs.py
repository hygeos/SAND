import pytest

from tests.generic import eval_login, eval_query, eval_download
from sandd.usgs import DownloadUSGS
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['LANDSAT-5'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param


def test_USGS_login(collec, level):
    eval_login(DownloadUSGS, collec, level)

def test_USGS_query(collec, level):
    eval_query(DownloadUSGS, collec, level,
               dtstart = datetime(2000, 12, 10),
               dtend = datetime(2005, 12, 10),
               geo = Point(119.514442, -8.411750))

def test_USGS_download(collec, level):
    eval_download(DownloadUSGS, collec, level,
                  dtstart = datetime(2000, 12, 10),
                  dtend = datetime(2005, 12, 10),
                  geo = Point(119.514442, -8.411750))
    