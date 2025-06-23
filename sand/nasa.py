import requests
import json

from pathlib import Path
from typing import Optional
from xmltodict import parse
from shapely import Point, Polygon
from requests.utils import requote_uri
from urllib.request import urlopen, Request
from urllib.parse import urlencode
from datetime import datetime, date

from core import log
from core.table import *
from core.static import interface
from core.files import filegen
from sand.base import BaseDownload, raise_api_error
from sand.results import Query
from sand.tinyfunc import *

# BASED ON : https://github.com/yannforget/landsatxplore/tree/master/landsatxplore



class DownloadNASA(BaseDownload):
    
    name = 'DownloadNASA'

    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the NASA CMR API (https://cmr.earthdata.nasa.gov/)

        Args:
            collection (str): collection name ('ECOSTRESS', 'VIIRS', etc.)

        Example:
            usgs = DownloadNASA('ECOSTRESS')
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750)
            )
            cds.download(ls.iloc[0], <dirname>, uncompress=True)
        """
        self.provider = 'nasa'
        super().__init__(collection, level)
        

    def _login(self):
        """
        Login to NASA with credentials storted in .netrc
        """
        log.info(f'No log required for NASA API (https://cmr.earthdata.nasa.gov/)')
        
    @interface
    def query(
        self,
        dtstart: Optional[date|datetime]=None,
        dtend: Optional[date|datetime]=None,
        geo=None,
        cloudcover_thres: Optional[int]=None,
        name_contains: Optional[list] = [],
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        other_attrs: Optional[list] = None,
        **kwargs
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
            This method can be decorated by cache_dataframe for storing the outputs.
            Example:
                cache_dataframe('cache_result.pickle')(cds.query)(...)
        """
        dtstart, dtend, geo = self._format_input_query(dtstart, dtend, geo)
        geo = flip_coords(change_lon_convention(geo))
        
        # Add provider constraint
        name_contains = self._complete_name_contains(name_contains)
        
        data = {}
        headers = {'Accept': 'application/json'}
        
        # Configure scene constraints for request
        date_range = dtstart.isoformat() + 'Z,'
        date_range += dtend.isoformat() + 'Z'
        data['temporal'] = date_range
        
        if isinstance(geo, Point):
            bbox = f"{geo.x},{geo.y},{geo.x},{geo.y}"
        elif isinstance(geo, Polygon):
            bounds = geo.bounds
            bbox = f"{bounds[1]},{bounds[0]},{bounds[3]},{bounds[2]}"
        data['bounding_box'] = bbox
        
        # Define check functions
        checker = []
        if name_contains: checker.append((check_name_contains, name_contains))
        if name_startswith: checker.append((check_name_startswith, name_startswith))
        if name_endswith: checker.append((check_name_endswith, name_endswith))
        if name_glob: checker.append((check_name_glob, name_glob))
        
        out = []
        for collec in self.api_collection:
            
            # Query NASA API
            log.debug(f'Query NASA API for collection {collec}')
            data['concept_id'] = collec
            data['page_size'] = 1000
            url = 'https://cmr.earthdata.nasa.gov/search/granules'
            url_encode = url + '?' + urlencode(data)
            urllib_req = Request(requote_uri(url_encode), headers=headers)
            urllib_response = urlopen(urllib_req, timeout=5, context=self.ssl_ctx)
            response = json.load(urllib_response)['feed']['entry']   
            
            # Filter products
            response = [p for p in response if self.check_name(p['title'], checker)]            
            
            # test if maximum number of returns is reached
            top = 1000
            if len(response) + len(out) >= top:
                log.error('The request led to the maximum number of results '
                        f'({len(response) + len(out)})', e=ValueError)
            
            for d in response:
                out.append({"id": d["id"], "name": d["producer_granule_id"],
                    **{k: d[k] for k in ['links','collection_concept_id']}})
        
        log.info(f'{len(response)} products has been found')
        return Query(out)
    
    @interface
    def download(self, product: dict, dir: Path|str, if_exists='skip', uncompress: bool=False) -> Path:
        """
        Download a product from NASA data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
            uncompress (bool, optional): If True, uncompress file if needed. Defaults to True.
        """
        target = Path(dir)/(product['name'])
        url = self._get(product['links'], '.h5', 'title', 'href')
        filegen(0, if_exists=if_exists)(self._download)(target, url)
        log.info(f'Product has been downloaded at : {target}')
        return target
    
    def _download(
        self,
        target: Path,
        url: str,
    ):
        """
        Wrapped by filegen
        """

        # Try to request server
        niter = 0
        response = self.session.get(url, allow_redirects=False)
        log.debug(f'Requesting server for {target.name}')
        while response.status_code in (301, 302, 303, 307) and niter < 5:
            log.debug(f'Download content [Try {niter+1}/5]')
            if 'Location' not in response.headers:
                raise ValueError(f'status code : [{response.status_code}]')
            url = response.headers['Location']
            # response = self.session.get(url, allow_redirects=False)
            response = self.session.get(url, verify=True, allow_redirects=True)
            niter += 1
        raise_api_error(response)

        # Download file
        log.debug('Start writing on device')
        filesize = int(response.headers["Content-Length"])
        pbar = log.pbar(log.lvl.INFO, total=filesize, unit_scale=True, unit="B", 
                        desc='writing', unit_divisor=1024, leave=False)
        with open(target, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(1024)
    
    @interface
    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        target = Path(dir)/(product['name'] + '.jpeg')
        url = self._get(product['links'], '.png', 'title', 'href')

        if not target.exists():
            filegen(0)(self._download)(target, url)

        log.info(f'Quicklook has been downloaded at : {target}')
        return target
    
    @interface
    def metadata(self, product):
        """
        Returns the product metadata including attributes and assets
        """
        req = self._get(product['links'], '.xml', 'title', 'href')
        meta = requests.get(req).text

        assert len(meta) > 0
        return parse(meta)
    
    def _retrieve_collec_name(self, collection):
        collecs = select(self.provider_prop,('SAND_name','=',collection),['level','collec'])
        try: collecs = select_cell(collecs,('level','=',self.level),'collec')
        except AssertionError: log.error(
            f'Level{self.level} products are not available for {self.collection}', e=KeyError)
        return collecs.split(' ')
    
    def _get(self, liste, name, in_key, out_key):
        for col in liste:
            if in_key not in col: continue
            if name in col[in_key]:
                return col[out_key]
        log.error(f'{name} has not been found', e=KeyError)
