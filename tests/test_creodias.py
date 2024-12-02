import pytest

from tests.generic import eval_login, eval_query, eval_download
from sandd.creodias import DownloadCreodias
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['SENTINEL-2'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    


def test_Creodias_login(collec, level):
    eval_login(DownloadCreodias, collec, level)

def test_Creodias_query(collec, level):
    eval_query(DownloadCreodias, collec, level,
               dtstart = datetime(2024, 1, 1),
               dtend = datetime(2024, 2, 1),
               geo = Point(119.514442, -8.411750),
               name_contains = ['_MSIL1C_'])

def test_Creodias_download(collec, level):
    eval_download(DownloadCreodias, collec, level,
                  dtstart = datetime(2024, 1, 1),
                  dtend = datetime(2024, 2, 1),
                  geo = Point(119.514442, -8.411750),
                  name_contains = ['_MSIL1C_'])
    