import pytest

from tests.generic import eval_login, eval_query, eval_download
from sand.nasa import DownloadNASA
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['ECOSTRESS'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    


def test_CNES_login(collec, level):
    eval_login(DownloadNASA, collec, level)

def test_CNES_query(collec, level):
    eval_query(DownloadNASA, collec, level,
               dtstart = datetime(2020, 1, 1),
               dtend = datetime(2020, 6, 1),
               geo = Point(119.514442, -8.411750)
               )

def test_CNES_download(collec, level):
    eval_download(DownloadNASA, collec, level,
                  dtstart = datetime(2020, 1, 1),
                  dtend = datetime(2020, 6, 1),
                  geo = Point(119.514442, -8.411750)
                  )
    