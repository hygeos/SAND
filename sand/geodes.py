from re import search
from pathlib import Path
from typing import Optional, Literal

from sand.constraint import Time, Geo, Name
from sand.base import BaseDownload, raise_api_error, check_too_many_matches
from sand.results import SandQuery, SandProduct
from sand.utils import write

from core import log
from core.network.auth import get_auth
from core.files.fileutils import filegen
from core.files.uncompress import uncompress


# [SOURCE] https://github.com/olivierhagolle/theia_download/tree/master
# https://geodes.cnes.fr/support/api/
class DownloadCNES(BaseDownload):
    """
    Python interface to the CNES Geodes Data Center (https://geodes-portal.cnes.fr/)
    """
    
    provider = 'geodes'
    safe_product = ['S1A','S2A','S2B']
    
    def __init__(self):
        super().__init__()

    def _log(self):
        """
        Login to CNES Geodes Data Center using credentials stored in .netrc
        """
        # Check if session is already set and set it up if not 
        if not hasattr(self, "session"):
            self._set_session()
            
        auth = get_auth("geodes.cnes.fr")     
        self.tokens = auth['password']
        log.debug('Log to API (https://geodes-portal.cnes.fr/)')
    
    
    def query(
        self,
        collection_sand: str = None,
        level: Literal[1,2,3] = 1,
        time: Time = None,
        geo: Geo = None,
        name: Name = None,
        cloudcover_thres: Optional[int] = None,
        api_collection: list[str] = None,
    ):
        """
        Product query on the Geodes Data Center
        """
        self._login()
        
        # Retrieve api collections based on SAND collections
        if api_collection is None:
            name_constraint = self._load_sand_collection_properties(collection_sand, level)
            api_collection = self.api_collection[0]
            
        # Format input time and geospatial constraints
        time = self._format_time(collection_sand, time)
        
        # Define or complement constraint on naming
        if name:
            name.add_contains(name_constraint)
        else:
            name = Name(contains=name_constraint)
        
        server_url = "https://geodes-portal.cnes.fr/api/stac/search"
        data = {'page':1, 'limit':500}
        query = {'dataset': {'in': self.api_collection}}
        
        # Time constraint
        if time.start:
            query['start_datetime'] = {'gte':time.start.isoformat()+'Z'}
        if time.end:
            query['end_datetime'] = {'lte':time.end.isoformat()+'Z'}
        
        # Spatial constraint
        if isinstance(geo, Geo.Point|Geo.Polygon): 
            data['bbox'] = geo.bounds
        if isinstance(geo, Geo.Tile):
            if geo.MGRS: query["location"] = geo.MGRS
            if geo.venus: query["grid:code"] = {'contains': geo.venus}
            
        if cloudcover_thres: 
            query['eo:cloud_cover'] = {"lte":cloudcover_thres}
        
        data['query'] = query
        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        response = self.session.post(server_url, json=data, verify=True)
        raise_api_error(response)
        
        # Filter products
        check_too_many_matches(response.json(), ['context','returned'], ['context','matched'])
        r = response.json()['features']
        response = [p for p in r if name.apply(p["properties"]['identifier'])]   

        out =  [
            SandProduct(
                product_id=d["properties"]["identifier"], index=d["id"],
                date=d['properties']['start_datetime'],
                metadata=d
            )
            for d in response
        ]
        
        log.info(f'{len(out)} products has been found')
        return SandQuery(out)

    def _dl(self, product: dict, dir: Path|str, if_exists='skip') -> Path:
        """
        Download a product from Geodes Datahub
        """
        self._login()
        
        # Extract download url
        links = product.metadata['assets']
        find = [l for l in links if search(product.product_id+'.*.zip',l)]
        log.check(len(find) == 1, "No download link for product found")
        
        # Check if product is a SAFE folder
        is_safe = any(product.product_id.startswith(p) for p in self.safe_product)
        if is_safe: 
            target = Path(dir)/find[0].replace('.zip','.SAFE')
        else:
            target = Path(dir)/find[0].replace('.zip','')
        
        dl_data = links[find[0]]
        filegen(0, if_exists=if_exists)(self._download)(target, dl_data, '.zip')
        log.info(f'Product has been downloaded at : {target}')
        return target

    def _download(
        self,
        target: Path,
        url: str,
        compression_ext: str = None
    ):
        """
        Internal method to handle the actual download of files from Geodes servers
        """
        
        # Compression file path
        dl_target = Path(str(target)+'.zip') if compression_ext else target
        
        # Download compressed file
        self.session.headers.update({"X-API-Key": self.tokens})
        response = self.session.get(url['href'], verify=True)
        raise_api_error(response)
        write(response, dl_target)
            
        # Uncompress archive
        if compression_ext:
            log.debug('Uncompress archive')
            path = uncompress(dl_target, target.parent)
            log.check(_name_difference(target.name, path.name) < 2, 
            f'target ({target}) is different from uncompressed file ({path})')
            path.rename(target)
            dl_target.unlink() 
    
    def _dl_file(self, product_id: str, dir: Path | str, api_collection: str = None) -> Path:
        """
        Download a specific product from Geodes by its identifier
        """
        self._login()
        
        log.warning('no api collection is required with new GEODES API')
            
        # Query and check if product exists
        server_url = f'https://geodes-portal.cnes.fr/api/stac/search'
        data = {'page':1, 'limit':1}
        data['query'] = {'identifier': {'contains':product_id}}
        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        
        response = self.session.post(server_url, json=data, verify=False)
        raise_api_error(response)        
        r = response.json()['features']
        log.check(len(r) > 0, f'No product named {product_id}')
        
        # Download the product
        out =  [
            SandProduct(
                product_id=d["properties"]["identifier"], index=d["id"],
                date=d['properties']['start_datetime'],
                metadata=d
            )
            for d in r
        ]
        assert len(out) == 1
        return self.download(out[0], dir, 'skip')

    def _qkl(self, product: dict, dir: Path|str):
        """
        Download a quicklook (preview image) of the product
        """
        self._login()
        
        links = product.metadata['assets']
        search = [l for l in links if 'quicklook' in l]
        log.check(len(search) == 1, "No download link for quicklook found")
        target = Path(dir)/search[0].split('/')[-1]
        url = links[search[0]]

        if not target.exists():
            filegen(0)(self._download)(target, url)
        
        log.info(f'Quicklook has been downloaded at : {target}')
        return target
          
    def _metadata(self, product: dict):
        """
        Returns the product metadata including attributes and assets
        """
        self._login()
        
        server_url = "https://geodes-portal.cnes.fr/api/stac/search"
        data = {'page':1, 'limit':5}
        data['query'] = {'identifier': {'contains':product.product_id}}

        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        response = self.session.post(server_url, json=data, verify=True)
        raise_api_error(response)

        return response.json()['features'][0]['properties']
    
    def _get(self, liste, name, in_key, out_key):
        """
        Internal helper to find a value in a list of dictionaries by matching keys
        """
        for col in liste:
            if in_key not in col: continue
            if name in col[in_key]:
                return col[out_key]
        log.error(f'{name} has not been found', e=KeyError)
    
def _name_difference(str1, str2) -> int:
    """
    Calculate the number of character differences between two strings.
    Compares position by position.
    """
    # Handle edge cases
    if str1 == str2:
        return 0
    
    max_len = max(len(str1), len(str2))
    diff_count = 0
    
    # Compare character by character
    for i in range(max_len):
        # Get character at position i, or None if index out of range
        char1 = str1[i] if i < len(str1) else None
        char2 = str2[i] if i < len(str2) else None
        
        if char1 != char2:
            diff_count += 1
    
    return diff_count