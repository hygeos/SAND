import requests
import json

from tqdm import tqdm
from pathlib import Path
from typing import Optional
from shapely import Point, Polygon
from requests.utils import requote_uri
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from datetime import datetime, time, date

from core.fileutils import filegen
from sand.base import request_get, BaseDownload, get_ssl_context

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore



class DownloadNASA(BaseDownload):
    
    name = 'DownloadNASA'
    
    collections = [
        'MODIS-AQUA',
        'MODIS-TERRA',
        'ECOSTRESS',
        'VIIRS',
        'ASTER',
    ]

    def __init__(self, collection: str, level: int):
        """
        Python interface to the NASA CMR API (https://cmr.earthdata.nasa.gov/)

        Args:
            collection (str): collection name ('LANDSAT-5', 'LANDSAT-7', etc.)

        Example:
            usgs = DownloadNASA('LANDSAT-5')
            # retrieve the list of products
            # using a json cache file to avoid reconnection
            ls = cache_json('query-S2.json')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
                name_contains='_MSIL1C_',
            )
            for p in ls:
                cds.download(p, <dirname>, uncompress=True)
        """
        assert collection in DownloadNASA.collections
        super().__init__(collection, level)
        

    def _login(self):
        """
        Login to NASA with credentials storted in .netrc
        """
        print(f'Log to API (https://cmr.earthdata.nasa.gov/)')
        

    def query(
        self,
        dtstart: Optional[date|datetime]=None,
        dtend: Optional[date|datetime]=None,
        geo=None,
        cloudcover_thres: Optional[int]=None,
        name_contains: Optional[list] = None,
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        other_attrs: Optional[list] = None,
    ):
        """
        Product query on the CMR NASA

        Args:
            dtstart and dtend (datetime): start and stop datetimes
            geo: shapely geometry. Examples:
                Point(lon, lat)
                Polygon(...)
            cloudcover_thres: Optional[int]=None,
            name_contains (list): list of substrings
            name_startswith (str): search for name starting with this str
            name_endswith (str): search for name ending with this str
            name_glob (str): match name with this string
            use_most_recent (bool): keep only the most recent processing baseline version
            other_attrs (list): list of other attributes to include in the output
                (ex: ['ContentDate', 'Footprint'])

        Note:
            This method can be decorated by cache_json for storing the outputs.
            Example:
                cache_json('cache_result.json')(cds.query)(...)
        """
        
        data = {'provider': 'LPCLOUD'}
        dataset = self._get_collection_id()
        headers = {'has_granules': 'True', 'Accept': 'application/json'}
        
        if isinstance(dtstart, date):
            date_range = datetime.combine(dtstart, time(0)).isoformat() + ','
            if isinstance(dtend, date):
                date_range += datetime.combine(dtend, time(0)).isoformat()
            else:
                date_range += datetime.now().isoformat()
            data['temporal'] = date_range
        
        # Configure scene constraints for request
        data['concept_id'] = dataset
        
        if isinstance(geo, Point):
            bbox = f"{geo.x},{geo.y},{geo.x},{geo.y}"
        elif isinstance(geo, Polygon):
            bounds = geo.bounds
            bbox = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"
        data['bounding_box'] = bbox
        
        # Request API for each dataset
        ssl_ctx = get_ssl_context()
        url = 'https://cmr.earthdata.nasa.gov/search/granules'
        url_encode = url + '?' + urlencode(data)
        urllib_req = Request(requote_uri(url_encode), headers=headers)
        urllib_response = urlopen(urllib_req, timeout=5, context=ssl_ctx)
        response = json.load(urllib_response)['feed']['entry']   
        
        # test if maximum number of returns is reached
        top = 1000
        if len(response) >= top:
            raise ValueError('The request led to the maximum number '
                    f'of results ({len(response)})')

        return [{"id": d["id"], "name": d["producer_granule_id"],
                 **{k: d[k] for k in ['links','collection_concept_id']}}
                for d in response]
    
    
    def download(self, product: dict, dir: Path|str, uncompress: bool=True) -> Path:
        """Download a product from copernicus data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): _description_
            uncompress (bool, optional): _description_. Defaults to True.
        """
        url = [l['href'] for l in product['links']][0]
        return self.download_base(url, product, dir, uncompress)
    
    
    def _download(
        self,
        target: Path,
        url: str,
    ):
        """
        Wrapped by filegen
        """
        pbar = tqdm(total=0, unit_scale=True, unit="B", 
                    unit_divisor=1024, leave=False)

        # Initialize session for download
        session = requests.Session()

        # Try to request server
        pbar.set_description(f'Requesting server: {target.name}')
        response = session.get(url, allow_redirects=False)
        niter = 0
        while response.status_code in (301, 302, 303, 307) and niter < 15:
            if response.status_code//100 == 5:
                raise ValueError(f'Got response code : {response.status_code}')
            if 'Location' not in response.headers:
                raise ValueError(f'status code : [{response.status_code}]')
            url = response.headers['Location']
            response = session.get(url, allow_redirects=False)
            niter += 1

        # Download file
        filesize = int(response.headers["Content-Length"])
        response = request_get(session, url, verify=False, allow_redirects=True)
        pbar = tqdm(total=filesize, unit_scale=True, unit="B",
                    unit_divisor=1024, leave=True)
        pbar.set_description(f"Downloading {target.name}")
        with open(target, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(1024)
    
    def _get_collection_id(self):
        if self.collection == 'MODIS-AQUA': return NotImplemented
        if self.collection == 'MODIS-TERRA': return NotImplemented
        if self.collection == 'ECOSTRESS': return 'C2595678497-LPCLOUD'
        if self.collection == 'VIIRS': return 'C2545310947-LPCLOUD'
        if self.collection == 'ASTER': return 'C2595678497-LPCLOUD'
