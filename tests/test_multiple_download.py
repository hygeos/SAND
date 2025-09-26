from tempfile import TemporaryDirectory
from sand.eumdac import DownloadEumDAC
from sand.sample_product import products
from core.monitor import Chrono
import pytest


@pytest.mark.skip("Not implemented")
@pytest.mark.parametrize('parallelized', [False, True])
def test_download_all(parallelized):
    with TemporaryDirectory() as tmpdir, Chrono():
        sensor = 'SENTINEL-3-OLCI-FR' 
        dl = DownloadEumDAC(sensor)
        ls = dl.query(**products[sensor]['level1'])
        dl.download_all(ls.iloc[:4], tmpdir, parallelized=parallelized)
