"""
This script aims to retrieve all Venµs tile name
"""

from numpy import savetxt
from datetime import timedelta, datetime
from argparse import ArgumentParser
from sand.constraint import Time
from sand.cnes import DownloadCNES
from core import log


parser = ArgumentParser()
parser.add_argument('--mission', choices=[f'VM{i+1}' for i in range(5)])
args = parser.parse_args()

start_date = datetime(2017,8,15)
timedelta = timedelta(weeks=1)
mission = args.mission
    
# Query GEODES API to get all available acquisitions 
extra = f' for mission {mission}' if mission else ''
log.info(f'Collecting all VENµS tile names'+extra)
dl = DownloadCNES()

# Function to check mission
if mission:
    checker = lambda x: x.metadata['properties']['platform'] == mission
else:
    checker = lambda x: True

tile_names = set()
log.set_lvl(log.lvl.WARNING)
for i in range(1000):
    start, end = start_date + i*timedelta, start_date + (i+1)*timedelta
    time = Time(start=start, end=end)
    ls = dl.query(collection_sand='VENUS', level=1, time=time)
    
    # Extract every venus tile names
    t = [
        product.metadata['properties']['grid:code'] 
        for product in ls.products if checker(product)
    ]
    tile_names = tile_names.union(set(t))
    
    if end >= datetime.now(): break
    
log.set_lvl(log.lvl.INFO)
log.info(f'{len(tile_names)} Venµs tiles found')

# Save into text file
txt_file = 'VENUS_tile_names.txt'
savetxt(txt_file, list(tile_names), '%s', delimiter=',')
log.info(f'List of tile names has been saved at : {txt_file}')