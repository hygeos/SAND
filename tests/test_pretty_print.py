from IPython.display import display
from sand.copernicus_dataspace import DownloadCDSE
from datetime import datetime
from shapely import Point


def test_query_results():
    dl = DownloadCDSE('SENTINEL-2-MSI')
    ls = dl.query(dtstart = datetime(2024, 1, 1), 
                  dtend = datetime(2024, 2, 1),
                  geo = Point(119.514442, -8.411750),
                  name_contains = ['_MSIL1C_'])
    display(ls)