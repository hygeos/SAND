from tempfile import TemporaryDirectory
from core.cache import cache_dataframe
from .conftest import savefig 
from pathlib import Path
from PIL import Image

import matplotlib.pyplot as plt

def eval_login(downloader, collec, level):
    downloader(collec, level)
    
def eval_collection(downloader):
    collec = downloader().get_available_collection()
    print(collec)   

def eval_download(downloader, collec, level, **kwargs):
    dl = downloader(collec, level)
    
    with TemporaryDirectory() as tmpdir:
        name_cache = Path(tmpdir)/f'test_{dl.name}_cache.pickle'
        ls = cache_dataframe(name_cache)(dl.query)(**kwargs)
        dl.download(ls.iloc[0], tmpdir, uncompress=True)
        assert len(list(Path(tmpdir).iterdir())) == 2
    
def eval_metadata(downloader, collec, level, **kwargs):
    dl = downloader(collec, level)
    
    with TemporaryDirectory() as tmpdir:
        name_cache = Path(tmpdir)/f'test_{dl.name}_cache.pickle'
        ls = cache_dataframe(name_cache)(dl.query)(**kwargs)
        meta = dl.metadata(ls.iloc[0])
        assert isinstance(meta, dict)
        print(meta)
    
def eval_quicklook(request, downloader, collec, level, **kwargs):
    dl = downloader(collec, level)
    
    with TemporaryDirectory() as tmpdir:
        name_cache = Path(tmpdir)/f'test_{dl.name}_cache.pickle'
        ls = cache_dataframe(name_cache)(dl.query)(**kwargs)
        quick = dl.quicklook(ls.iloc[0], tmpdir)
        assert Path(quick).exists()
        
        img = Image.open(quick)
        plt.imshow(img)
        savefig(request)

def eval_download_file(downloader, product_id):
    dl = downloader()
    with TemporaryDirectory() as tmpdir:
        quick = dl.download_file(product_id, tmpdir)
        assert Path(quick).exists()
        assert product_id in quick