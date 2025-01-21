import requests
import json

from tqdm import tqdm
from pathlib import Path
from typing import Optional
from xmltodict import parse
from shapely import Point, Polygon
from requests.utils import requote_uri
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from datetime import datetime, time, date

from core import log
from core.table import *
from core.fileutils import filegen
from sand.base import request_get, BaseDownload
from sand.results import Query

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore



class DownloadNASA(BaseDownload):
    
    name = 'DownloadNASA'
    
    collections = [
        'ECOSTRESS',
        'EMIT',
        'MODIS-AQUA-HR',
        'MODIS-AQUA-LR',
        'MODIS-TERRA-HR',
        'MODIS-TERRA-LR',
        'SENTINEL-6-HR',
        'SENTINEL-6-LR',
        'VIIRS',
    ]

    def __init__(self, collection: str = None, level: int = 1):
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
        self.available_collection = DownloadNASA.collections
        self.table_collection = Path(__file__).parent/'collections'/'nasa.csv'
        super().__init__(collection, level)
        

    def _login(self):
        """
        Login to NASA with credentials storted in .netrc
        """
        log.info(f'Log to API (https://cmr.earthdata.nasa.gov/)')
        

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
        
        data = {}
        headers = {'Accept': 'application/json'}
        
        # Configure scene constraints for request        
        if isinstance(dtstart, date):
            date_range = datetime.combine(dtstart, time(0)).isoformat() + 'Z,'
            if isinstance(dtend, date):
                date_range += datetime.combine(dtend, time(0)).isoformat() + 'Z'
            else:
                date_range += datetime.now().isoformat() + 'Z'
            data['temporal'] = date_range
        
        if isinstance(geo, Point):
            bbox = f"{geo.x},{geo.y},{geo.x},{geo.y}"
        elif isinstance(geo, Polygon):
            bounds = geo.bounds
            bbox = f"{bounds[1]},{bounds[0]},{bounds[3]},{bounds[2]}"
        data['bounding_box'] = bbox
        
        # Request API for each dataset
        out = []
        for collec in self.collection:
            data['concept_id'] = collec
            data['page_size'] = 1000
            url = 'https://cmr.earthdata.nasa.gov/search/granules'
            url_encode = url + '?' + urlencode(data)
            urllib_req = Request(requote_uri(url_encode), headers=headers)
            urllib_response = urlopen(urllib_req, timeout=5, context=self.ssl_ctx)
            response = json.load(urllib_response)['feed']['entry']   
            
            # test if maximum number of returns is reached
            top = 1000
            if len(response) >= top:
                raise ValueError('The request led to the maximum number '
                        f'of results ({len(response)})')
            
            for d in response:
                out.append({"id": d["id"], "name": d["producer_granule_id"],
                    **{k: d[k] for k in ['links','collection_concept_id']}})
        
        return Query(out)

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        target = Path(dir)/(product['name'] + '.jpeg')
        url = self._get(product['links'], '.png', 'title', 'href')

        if not target.exists():
            filegen(0)(self._download)(target, url)

        return target
    
    def download(self, product: dict, dir: Path|str, uncompress: bool=False) -> Path:
        """Download a product from copernicus data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): _description_
            uncompress (bool, optional): _description_. Defaults to False.
        """
        url = self._get(product['links'], '.h5', 'title', 'href')
        return self.download_base(url, product, dir, False)
    
    
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

    def metadata(self, product):
        """
        Returns the product metadata including attributes and assets
        """
        req = self._get(product['links'], '.xml', 'title', 'href')
        meta = requests.get(req).text

        assert len(meta) > 0
        return parse(meta)
    
    def _retrieve_collec_name(self, collection):
        correspond = read_csv(self.table_collection)
        collecs = select(correspond,('level','=',self.level),['SAND_name','collec'])
        collecs = select_cell(collecs,('SAND_name','=',collection),'collec')
        return collecs.split(' ')
    
    def _get(self, liste, name, in_key, out_key):
        for col in liste:
            if in_key not in col: continue
            if name in col[in_key]:
                return col[out_key]
        raise FileNotFoundError
