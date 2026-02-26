from argparse import ArgumentParser
from pandas import concat
from pathlib import Path

from numpy import unique
from sand.sample_product import products
from sand._cli_cfg import SearchCfg

from core import log
from core.import_utils import import_module
from core.ascii_table import ascii_table
from core.table import read_csv
from core.fuzzy import search


def entry():
    
    # Command line arguments
    parser = ArgumentParser(description="SAND search engine CLI tool", 
        epilog="Made by HYGEOS."
    )
    subs = parser.add_subparsers(dest="command", required=True)
    
    
    # > download command
    dl = subs.add_parser(name="download", help="download product with SAND downloader")
    dl.add_argument("--from", action="store", help="Provider to query")
    dl.add_argument("--mute", action="store_true", help="Disable verbose")
    dl.add_argument("--dir", action="store", help="Directory where to store downloaded product")
    dl.add_argument(
        "product_ids",
        nargs="+",  # Captures ALL remaining arguments
        help="Identifiers of product to download",
        default=None
    )
    
    
    # > search command
    cmd = subs.add_parser(name="search", help="search products in the datasets interfaced by SAND")
    cmd.add_argument(
        "keywords", 
        nargs="+",  # Captures ALL remaining arguments
        help="Keywords to search for in the database",
        default=None,
    )
    
    cmd.add_argument("--debug", action="store_true", help="Debug mode (developper)", default=False)
    cmd.add_argument("--minimum", "--min", action="store", help="Minimum match score to consider [20-100]", 
        default=None, metavar="match_threshold"
    )
    
    cmd.add_argument("--from", action="store", help="Provider selection (Like usgs, cdse, etc..)", 
        default=None, nargs="+", metavar="source",
    )
    
    cmd.add_argument("--level", action="store", help="Level selection (Like L1, L2, etc..)", 
        default=None, nargs="+", metavar="source",
    )
        
    # Create a mutually exclusive group
    mode_group = cmd.add_mutually_exclusive_group()
    mode_group.add_argument("--exact", "-e",        action="store_true", help="Exact matching")
    mode_group.add_argument("--strict", "-s",       action="store_true", help="Strict matching")

    width_group = cmd.add_mutually_exclusive_group()
    width_group.add_argument("--large", "-l", action="store_true", help="Display columns to their max width", default=False)
    
    cmd.add_argument("--nocolor", "-n", action="store_true", help="Disable color output", default=False)
    cmd.add_argument("--compact", "-c", action="store_true", help="Compact layout", default=False)
        
    
    # > search command
    sample = subs.add_parser(name="sample", help="get a sample product identifier")
    
    args = parser.parse_args()
    
    if args.command == 'search':
        
        cfg = SearchCfg()
        
        # Concatenate all csv files
        providers = []
        collection_dir = Path(__file__).parent/'collections'
        for path in collection_dir.glob('*.csv'):
            collec = read_csv(path).assign(provider=path.stem)
            providers.append(collec)
        
        # Open table with sensor information
        sensor_df = read_csv(Path(__file__).parent/'sensors.csv')
        
        # Process final table
        df = concat(providers, ignore_index=True)
        df = df.rename(columns={'SAND_name': 'Name'})
        df = df.merge(sensor_df, on='Name', how='left')
        df = df.drop(['contains', 'collec', 'url'], axis=1)
        df['level'] = df['level'].apply(lambda x: f'L{x}')
        df['end_date'] = df['end_date'].apply(lambda x: x if x != 'x' else '-')
        
        if getattr(args, 'from'):
            df = df[df['provider'].isin(getattr(args, 'from'))]
        
        if args.level:
            df = df[df['level'].isin(args.level)]
            
        # Apply selection based on fuzzy search result
        if args.keywords != ['']:
            keywords = [part for k in args.keywords for part in k.split('-')]
            selection = unique([n for n,_ in search(keywords, df['Name'])])
            df = df[df['Name'].isin(selection)]
        
        # Display final results
        if len(df) == 0:
            print(log.rgb.orange, '> No result found.')
            exit()
        
        df = df.sort_values(by='Name')
        ascii_table(df, colors=cfg.colors).print()
    
    if args.command == 'download':
        
        # Initialize all provider downloaders
        providers = {
            'cdse': 'sand.copernicus_dataspace.DownloadCDSE',
            'eumdac': 'sand.eumdac.DownloadEumDAC',
            'cnes': 'sand.cnes.DownloadCNES',
            'nasa': 'sand.nasa.DownloadNASA',
            'usgs': 'sand.usgs.DownloadUSGS',
        }
        
        # Select deserved downloader
        if getattr(args, 'from') not in providers.keys():
            print(log.rgb.red, f'> Provider is not part of {list(providers.keys())}')
            exit()
        Downloader = import_module(providers[getattr(args, 'from')])
        
        # Disable verbose messages
        if args.mute:
            log.set_lvl(log.lvl.WARNING)
        
        # Instantiate and query API
        prod_ids, directory = list(args.product_ids), args.dir
        for prod in prod_ids:
            Downloader().download_file(prod, directory)
            print(log.rgb.green, f'> {prod} successfully download.')
    
    if args.command == 'sample':
        print(log.rgb.orange, '> Implementation of sample command is on going.')