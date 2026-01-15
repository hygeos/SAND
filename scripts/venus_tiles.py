from numpy import unique, savetxt, concatenate
from datetime import timedelta, datetime
from sand.cnes import DownloadCNES
from core import log


start_date = datetime(2017,8,15)
timedelta = timedelta(weeks=1)

for collec in ['VENUS','VENUS-VM5']:
    
    # Query GEODES API to get all available acquisitions 
    log.info(f'Collecting all VENµS tile names for {collec}')
    dl = DownloadCNES(collec)
    
    tile_names = []
    log.set_lvl(log.lvl.WARNING)
    for i in range(1000):
        start, end = start_date + i*timedelta, start_date + (i+1)*timedelta
        ls = dl.query(start, end, other_attrs=['properties'])
    
        # Extract every venus tile names
        t = [rows['properties']['grid:code'] for i,rows in ls.iterrows()]
        tile_names.append(unique(t))
        
        if end >= datetime.now(): break
        
    log.set_lvl(log.lvl.INFO)
    tile_names = unique(concatenate(tile_names))
    
    # Save into text file
    txt_file = f'{collec}_tile_names.txt'
    savetxt(txt_file, tile_names, '%s', delimiter=',')
    log.info(f'List of tile names has been saved at : {txt_file}')