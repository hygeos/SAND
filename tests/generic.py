from tempfile import TemporaryDirectory
from core.cache import cache_json
from pathlib import Path


def eval_login(downloader, collec, level):
    downloader(collec, level)

def eval_query(downloader, collec, level, **kwargs):
    dl = downloader(collec, level)
    
    with TemporaryDirectory() as tmpdir:
        name_cache = Path(tmpdir)/f'test_{dl.name}_cache.json'
        ls = cache_json(name_cache)(dl.query)(**kwargs)
        assert len(ls) != 0, 'No product found'

def eval_download(downloader, collec, level, **kwargs):
    dl = downloader(collec, level)
    
    with TemporaryDirectory() as tmpdir:
        name_cache = Path(tmpdir)/f'test_{dl.name}_cache.json'
        ls = cache_json(name_cache)(dl.query)(**kwargs)
        dl.download(ls[0], tmpdir, uncompress=True)
        assert len(list(Path(tmpdir).iterdir())) == 2