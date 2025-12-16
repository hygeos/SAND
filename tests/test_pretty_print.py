from sand.copernicus_dataspace import DownloadCDSE
from sand.constraint import Time, Geo, Name


def test_query_results():
    dl = DownloadCDSE()
    ls = dl.query(
        collection_sand='SENTINEL-2-MSI',
        time = Time('2024-01-01', '2024-02-01'),
        geo = Geo.Point(-8.5, 119),
        name = Name(contains=['_MSIL1C_'])
    )
    print(ls)