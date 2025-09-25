from tempfile import TemporaryDirectory
from core.files import cache_dataframe
from .conftest import savefig 
from pathlib import Path
from PIL import Image

import matplotlib.pyplot as plt


def eval_login(downloader):
    downloader._login()
    
def eval_collection(downloader):
    collec = downloader.get_available_collection()
    print(collec)   

def eval_download(downloader, collec, level, **kwargs):     
    kwargs.update(collection_sand=collec, level=level) 
    with TemporaryDirectory() as tmpdir:
        name_cache = Path(tmpdir)/f'test_{downloader.provider}_cache.pickle'
        ls = cache_dataframe(name_cache)(downloader.query)(**kwargs)
        assert all(c in ls.columns for c in ['id', 'name'])
        downloader.download(ls.iloc[0], tmpdir)
        assert len(list(Path(tmpdir).iterdir())) == 2
    
def eval_metadata(downloader, collec, level, **kwargs):    
    kwargs.update(collection_sand=collec, level=level)
    with TemporaryDirectory() as tmpdir:
        name_cache = Path(tmpdir)/f'test_{downloader.provider}_cache.pickle'
        ls = cache_dataframe(name_cache)(downloader.query)(**kwargs)
        meta = downloader.metadata(ls.iloc[0])
        assert isinstance(meta, dict)
        print(meta)
    
def eval_quicklook(request, downloader, collec, level, **kwargs):     
    kwargs.update(collection_sand=collec, level=level) 
    with TemporaryDirectory() as tmpdir:
        name_cache = Path(tmpdir)/f'test_{downloader.provider}_cache.pickle'
        ls = cache_dataframe(name_cache)(downloader.query)(**kwargs)
        quick = downloader.quicklook(ls.iloc[0], tmpdir)
        assert quick.exists()
        
        img = Image.open(quick)
        plt.imshow(img)
        savefig(request)

def eval_download_file(downloader, product_id):
    with TemporaryDirectory() as tmpdir:
        quick = downloader.download_file(product_id, tmpdir)
        assert Path(quick).exists()
        assert product_id in quick.name