from sand.sample_product import products
from tempfile import TemporaryDirectory
from contextlib import contextmanager
from core import env

import argparse
import sand

dl = {
    'CDSE': sand.DownloadCDSE,
    'EUMDAC': sand.DownloadEumDAC,
    'CNES': sand.DownloadCNES,
    'NASA': sand.DownloadNASA,
    'USGS': sand.DownloadUSGS,
}

parser = argparse.ArgumentParser('try_dl', description='Try to download an example of a product')
parser.add_argument('-s', '--sensor', help='Sensor to download', choices=list(products.keys()))
parser.add_argument('-d', '--dl', choices=list(dl.keys()), help='Provider to query')
parser.add_argument('-l', '--level', help='Product level', choices=['1','2'])
parser.add_argument('-v', '--verbose', action='store_true')
parser.add_argument('-k', '--keep', action='store_true')
args = parser.parse_args() 

@contextmanager
def get_tempdir(keep):
    if not keep: 
        with TemporaryDirectory() as tmpdir: yield tmpdir
    else: yield env.getdir('DIR_SAMPLES')

with get_tempdir(args.keep) as tmpdir:
    # d = dl[args.dl]()
    # params = products[args.sensor]['constraint']
    # params.update(collection_sand=args.sensor, level=int(args.level))
    d = dl['USGS']()
    params = products['LANDSAT-1-MSS']['constraint']
    params.update(collection_sand='LANDSAT-1-MSS', level=1)
    params = dict(collection_sand='LANDSAT-1-MSS', level=1)
    ls = d.query(**params)
    d.download(ls[0], tmpdir)