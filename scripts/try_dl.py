from sand.sample_product import products
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
args = parser.parse_args() 

d = dl[args.dl](args.sensor, int(args.level))
params = products[args.sensor]
assert f'level{args.level}' in params
ls = d.query(**params[f'level{args.level}'])
d.download(ls.iloc[0], '.')