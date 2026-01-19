from tempfile import TemporaryDirectory
import pytest

from sand.sample_product import products
from sand.copernicus_dataspace import DownloadCDSE
from sand.eumdac import DownloadEumDAC
from sand.cnes import DownloadCNES
from sand.nasa import DownloadNASA
from sand.usgs import DownloadUSGS


@pytest.mark.parametrize('downloader', [DownloadCDSE, DownloadCNES])
def test_double_dir_for_msi(downloader):
    with TemporaryDirectory() as tmpdir:
        dl = downloader()
        prod_id = products['SENTINEL-2-MSI']['l1_product']
        out = dl.download_file(prod_id, tmpdir)
        assert out.exists()
        assert not (out/out.name).exists()

def test_landsat_product_is_dir():
    with TemporaryDirectory() as tmpdir:
        dl = DownloadUSGS()
        prod_id = products['LANDSAT-9-OLI']['l1_product']
        out = dl.download_file(prod_id, tmpdir)
        assert out.exists()
        assert out.is_dir()

def test_venus_download_file_with_cnes():
    with TemporaryDirectory() as tmpdir:
        dl = DownloadCNES()
        prod_id = products['VENUS']['l1_product']
        out = dl.download_file(prod_id, tmpdir)
        assert out.exists()
        assert out.name.startswith(prod_id)