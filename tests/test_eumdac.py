import pytest

from tests.generic import eval_login, eval_query, eval_download
from sand.eumdac import DownloadEumDAC
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['MSG-SEVIRI'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param


def test_EumDAC_login(collec, level):
    eval_login(DownloadEumDAC, collec, level)

def test_EumDAC_query(collec, level):
    eval_query(DownloadEumDAC, collec, level,
               dtstart = datetime(2022, 1, 1),
               dtend = datetime(2022, 1, 10),
               geo = Point(10, 12))

def test_EumDAC_download(collec, level):
    eval_download(DownloadEumDAC, collec, level,
                  dtstart = datetime(2024, 1, 1),
                  dtend = datetime(2024, 1, 10),
                  geo = Point(10, 12))