from sand.copernicus_dataspace import DownloadCDSE
from sand.base import RequestsError
from sand.sample_product import products
from shapely import Point

import pytest


@pytest.fixture(params=['SENTINEL-3-OLCI-FR'])
def collec(request):
    return request.param

@pytest.fixture
def constraint(collec):
    return products[collec]['level1']

def test_invalid_level(collec):
    with pytest.raises(KeyError):
        DownloadCDSE(collec,2)

@pytest.mark.parametrize('lonlat', [(-100,-30), (-30,-100)])
def test_latlon_convention(collec, constraint, lonlat):
    constraint['geo'] = Point(*lonlat)
    with pytest.raises(RequestsError):
        dl = DownloadCDSE(collec,1)
        dl.query(**constraint)
        