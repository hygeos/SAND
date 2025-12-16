from sand.copernicus_dataspace import DownloadCDSE
from sand.sample_product import products
from sand.constraint import _change_lon_convention

from core.table import read_csv
from shapely import Point, Polygon
from pathlib import Path

import pytest


@pytest.fixture(params=['SENTINEL-3-OLCI-FR'])
def collec(request):
    return request.param

@pytest.fixture
def constraint(collec):
    return products[collec]['constraint']

def test_invalid_level(collec):
    with pytest.raises(ReferenceError):
        DownloadCDSE().query(collec,3)

@pytest.mark.parametrize('center, value',[(0, -150), (180, 210)])
def test_latlon_change_convention(center, value):
    assert _change_lon_convention(210, center) == value
        
@pytest.mark.parametrize('provider', ['cdse','eumdac','nasa','geodes','usgs'])
def test_provider_file(provider):
    p = str(Path(__file__).parent.parent/'sand'/'collections'/'{}.csv')
    read_csv(p.format(provider))