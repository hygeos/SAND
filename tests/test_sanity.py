from sand.copernicus_dataspace import DownloadCDSE
from sand.sample_product import products
from sand.base import RequestsError
from sand.tinyfunc import change_lon_convention

from core.table import read_csv
from shapely import Point, Polygon
from pathlib import Path

import pytest


@pytest.fixture(params=['SENTINEL-3-OLCI-FR'])
def collec(request):
    return request.param

@pytest.fixture
def constraint(collec):
    return products[collec]['level1']

@pytest.mark.skip('Find another sensor')
def test_invalid_level(collec):
    with pytest.raises(KeyError):
        DownloadCDSE(collec,2)

@pytest.mark.parametrize('lonlat', [(-200,-30), (-30,-100)])
def test_latlon_convention(collec, constraint, lonlat):
    constraint['geo'] = Point(*lonlat)
    with pytest.raises(RequestsError):
        dl = DownloadCDSE(collec,1)
        dl.query(**constraint)

@pytest.mark.parametrize('center',[0, 180])
@pytest.mark.parametrize('a, b',[
    (Point(200,30), Point(-160,30)), 
    (Polygon.from_bounds(200,30,210,40), Polygon.from_bounds(-160,30,-150,40))
])
def test_latlon_change_convention(center, a, b):
    inputs, output = (a,b) if center == 0 else (b,a)
    assert change_lon_convention(inputs, center) == output
        
@pytest.mark.parametrize('provider', ['cdse','eumdac','nasa','geodes','usgs'])
def test_provider_file(provider):
    p = str(Path(__file__).parent.parent/'sand'/'collections'/'{}.csv')
    read_csv(p.format(provider))