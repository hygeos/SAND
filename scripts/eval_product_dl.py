from sand.constraint import Time
from sand.sample_product import products
from tempfile import TemporaryDirectory
from core.import_utils import import_module
from core import log
import argparse

log.set_lvl(log.lvl.WARNING)


dl = {
    'CDSE': 'sand.copernicus_dataspace.DownloadCDSE',
    'EUMDAC': 'sand.eumdac.DownloadEumDAC',
    'CNES': 'sand.cnes.DownloadCNES',
    'NASA': 'sand.nasa.DownloadNASA',
    'USGS': 'sand.usgs.DownloadUSGS',
}

# CLI interface
parser = argparse.ArgumentParser(description="Check which product is available")
parser.add_argument('-p', '--provider',
                    action = 'store',
                    help = 'Name of the provider to check',
                    choices = list(dl.keys()),
                    type = str)
parser.add_argument('-c', '--check_collec',
                    action = 'store_true',
                    help = 'Option to only check that collection is not empty')
args = parser.parse_args()


# Import only necessary downloader
if args.provider: 
    dl = {args.provider: import_module(dl[args.provider])}
else:
    dl = {p: import_module(d) for p,d in dl.items()}

# Download every file in temporary directory
with TemporaryDirectory() as tmpdir:
    for name, provider in dl.items():
        
        # Initialise downloader
        print(log.rgb.cyan, '-'*5,f' Provider : {name} ','-'*5)
        dl = provider()
        collec_df = dl.get_available_collection()
        
        error_msg_end = ' failed at {} with this message : {}'
        for collec in collec_df['Name']:
            
            if collec not in products: 
                continue
            
            params = products[collec]['constraint']
            for level in [1,2,3]:
                
                msg_start = f'Download of {collec} {level} with provider {name}'
                error_msg = msg_start + error_msg_end
                
                # Query
                try: 
                    if args.check_collec:
                        if name in ['CNES','NASA','CDSE']:
                            params = dict(time=Time('1970-01-01', '2025-01-01'))
                            
                    ls = dl.query(collection_sand=collec, level=level, **params)
                except ReferenceError as e:
                    continue
                except Exception as e: 
                    print(log.rgb.red, error_msg.format('query',e))
                    continue
                
                if len(ls)==0: 
                    print(log.rgb.red, error_msg.format('query','No product found by the query'))
                    continue
                
                # Download
                try: 
                    dl.download(ls[0], tmpdir)
                except Exception as e: 
                    print(log.rgb.red, error_msg.format('download',e))
                    continue
            
                print(log.rgb.green, msg_start+' successed')