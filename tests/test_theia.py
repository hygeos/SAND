import pytest

from tests.generic import eval_login, eval_query, eval_download
from sandd.theia import DownloadTHEIA
from datetime import datetime
from shapely import Point


@pytest.fixture(params=['VENUS'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    


def test_CNES_login(collec, level):
    eval_login(DownloadTHEIA, collec, level)

def test_CNES_query(collec, level):
    eval_query(DownloadTHEIA, collec, level,
               dtstart = datetime(2020, 1, 1),
               dtend = datetime(2020, 6, 1),
               venus_site='NARYN')

def test_CNES_download(collec, level):
    eval_download(DownloadTHEIA, collec, level,
                  dtstart = datetime(2020, 1, 1),
                  dtend = datetime(2020, 6, 1),
                  venus_site='NARYN')
    