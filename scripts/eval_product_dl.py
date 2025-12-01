from sand.sample_product import products
from tempfile import TemporaryDirectory
from core import log
import argparse
import sand

log.silence(sand)
log.set_lvl(log.lvl.INFO)


dl = {
    'CDSE': sand.DownloadCDSE,
    'EUMDAC': sand.DownloadEumDAC,
    'CNES': sand.DownloadCNES,
    'NASA': sand.DownloadNASA,
    'USGS': sand.DownloadUSGS,
}

parser = argparse.ArgumentParser(description="Check which product is available")
parser.add_argument('-p', '--provider',
                    action = 'store',
                    help = 'Name of the provider to check',
                    choices = list(dl.keys()),
                    type = str)

args = parser.parse_args()

if args.provider: 
    dl = {args.provider: dl[args.provider]}

with TemporaryDirectory() as tmpdir:
    for name, provider in dl.items():
        
        log.info(log.rgb.cyan, '-'*5,f' Provider : {name} ','-'*5)
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
                    ls = dl.query(collection_sand=collec, level=level, **params)
                except ReferenceError as e:
                    continue
                except Exception as e: 
                    log.info(log.rgb.red, error_msg.format('query',e))
                    continue
                
                if len(ls)==0: 
                    log.info(log.rgb.red, error_msg.format('query','No product found by the query'))
                    continue
                
                # Download
                try: 
                    dl.download(ls.iloc[0], tmpdir)
                except Exception as e: 
                    log.info(log.rgb.red, error_msg.format('download',e))
                    continue
            
                log.info(log.rgb.green, msg_start+' successed')