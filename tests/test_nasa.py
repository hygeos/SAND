import pytest

from tests.generic import *
from sand.nasa import DownloadNASA
from datetime import datetime
from shapely import Polygon


@pytest.fixture(params=['ECOSTRESS'])
def collec(request):
    return request.param
    
@pytest.fixture(params=[1])
def level(request):
    return request.param
    


def test_login(collec, level):
    eval_login(DownloadNASA, collec, level)

def test_collection():
    eval_collection(DownloadNASA)

def test_download(collec, level):
    eval_download(DownloadNASA, collec, level,
                  dtstart = datetime(2023, 10, 20),
                  dtend = datetime(2023, 11, 14),
                  geo = Polygon(((34.210026,-120.295181),
                                 (34.210026,-119.526215),
                                 (35.225021,-119.526215),
                                 (35.225021,-120.295181),
                                 (34.210026,-120.295181)))
                  )

def test_metadata(collec, level):
    eval_metadata(DownloadNASA, collec, level,
                  dtstart = datetime(2023, 10, 20),
                  dtend = datetime(2023, 11, 14),
                  geo = Polygon(((34.210026,-120.295181),
                                 (34.210026,-119.526215),
                                 (35.225021,-119.526215),
                                 (35.225021,-120.295181),
                                 (34.210026,-120.295181)))
                  )
    

def test_quicklook(request, collec, level):
    eval_quicklook(request, DownloadNASA, collec, level,
                  dtstart = datetime(2023, 10, 20),
                  dtend = datetime(2023, 11, 14),
                  geo = Polygon(((34.210026,-120.295181),
                                 (34.210026,-119.526215),
                                 (35.225021,-119.526215),
                                 (35.225021,-120.295181),
                                 (34.210026,-120.295181)))
                  )
    
    