import pytest

from tests.generic import *
from sand.eumdac import DownloadEumDAC
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['SENTINEL-3-OLCI-FR'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param


def test_login(collec, level):
    eval_login(DownloadEumDAC, collec, level)

def test_collection():
    eval_collection(DownloadEumDAC)

def test_download(collec, level):
    eval_download(DownloadEumDAC, collec, level,
                  dtstart = datetime(2025, 1, 1),
                  dtend = datetime(2025, 2, 1),
                  geo = Point(10, 12))

def test_metadata(collec, level):
    eval_metadata(DownloadEumDAC, collec, level,
                  dtstart = datetime(2025, 1, 1),
                  dtend = datetime(2025, 2, 1),
                  geo = Point(10, 12))
    
def test_quicklook(request, collec, level):
    eval_quicklook(request, DownloadEumDAC, collec, level,
                   dtstart = datetime(2025, 1, 1),
                   dtend = datetime(2025, 2, 1),
                   geo = Point(10, 12))
    