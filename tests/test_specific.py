from tempfile import TemporaryDirectory
import pytest

from sand.sample_product import products
from sand.copernicus_dataspace import DownloadCDSE
from sand.eumdac import DownloadEumDAC
from sand.cnes import DownloadCNES
from sand.nasa import DownloadNASA
from sand.usgs import DownloadUSGS


def test_double_dir_for_msi():
    with TemporaryDirectory() as tmpdir:
        dl = DownloadCDSE()
        prod_id = products['SENTINEL-2-MSI']['l1_product']
        out = dl.download_file(prod_id, tmpdir)
        assert out.exists()
        assert not (out/out.name).exists()

def test_different_archive_product():
    with TemporaryDirectory() as tmpdir:
        dl = DownloadCDSE()
        prod_id = products['SENTINEL-2-MSI']['l1_product']
        dl.download_file(prod_id, tmpdir)
        
        with pytest.raises(AssertionError):
            dl = DownloadCNES()
            prod_id = products['SENTINEL-2-MSI']['l1_product']
            dl.download_file(prod_id, tmpdir)

@pytest.mark.parametrize('collec',['LANDSAT-8-OLI','LANDSAT-9-OLI'])
def test_landsat_product_is_dir(collec):
    with TemporaryDirectory() as tmpdir:
        dl = DownloadUSGS()
        prod_id = products[collec]['l1_product']
        out = dl.download_file(prod_id, tmpdir)
        assert out.exists()
        assert out.is_dir()