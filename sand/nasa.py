import requests
import json

from pathlib import Path
from typing import Optional
from shapely import Point, Polygon
from tempfile import TemporaryDirectory
from urllib.parse import urlencode
from datetime import datetime, date

from core import log
from core.files import filegen
from core.static import interface
from core.table import read_xml, select, select_cell
from core.geo.product_name import get_pattern, get_level

from sand.base import BaseDownload, raise_api_error, check_too_many_matches
from sand.results import Query
from sand.tinyfunc import (
    change_lon_convention,
    check_name_contains, 
    check_name_glob,
    check_name_endswith,
    check_name_startswith,
)

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
            cds.download(ls.iloc[0], <dirname>)
        """
        self.provider = 'nasa'
        super().__init__(collection, level)
        

    def _login(self):
        """
        Login to NASA with credentials storted in .netrc
        """
        log.debug(f'No login required for NASA API (https://cmr.earthdata.nasa.gov/)')
        
    @interface
    def query(
        self,
        dtstart: Optional[date|datetime]=None,
        dtend: Optional[date|datetime]=None,
        geo=None,
        cloudcover_thres: Optional[float]=None,
        name_contains: Optional[list] = [],
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        other_attrs: Optional[list] = [],
        **kwargs
    ):
        """
        Product query on the CMR NASA

        Args:
            dtstart and dtend (datetime): start and stop datetimes
            geo: shapely geometry with 0<=lon<360 and -90<=lat<90. Examples:
                Point(lon, lat)
                Polygon(...)
            cloudcover_thres (int): Upper bound for cloud cover in percentage, 
            name_contains (list): list of substrings
            name_startswith (str): search for name starting with this str
            name_endswith (str): search for name ending with this str
            name_glob (str): match name with this string
            other_attrs (list): list of other attributes to include in the output
                (ex: ['ContentDate', 'Footprint'])

        Note:
            This method can be decorated by cache_dataframe for storing the outputs.
            Example:
                cache_dataframe('cache_result.pickle')(cds.query)(...)
        """
        dtstart, dtend, geo = self._format_input_query(dtstart, dtend, geo)
        if geo: geo = change_lon_convention(geo)
        
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
            bbox = f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}"
        if geo: data['bounding_box'] = bbox
        
        # Define check functions
        checker = []
        if name_contains: checker.append((check_name_contains, name_contains))
        if name_startswith: checker.append((check_name_startswith, name_startswith))
        if name_endswith: checker.append((check_name_endswith, name_endswith))
        if name_glob: checker.append((check_name_glob, name_glob))
        
        # Add constraint for cloud cover
        if cloudcover_thres: data['cloud_cover'] = f",{cloudcover_thres}"
            
        out = []
        for collec in self.api_collection:
            
            # Query NASA API
            log.debug(f'Query NASA API for collection {collec}')
            data['concept_id'] = collec
            data['page_size'] = 1000
            url = 'https://cmr.earthdata.nasa.gov/search/granules'
            url_encode = url + '?' + urlencode(data)
            response = self.session.post(url_encode, headers=headers, verify=True)
            check_too_many_matches(response.json())
            response = response.json()['feed']['entry']   
            
            # Filter products
            response = [p for p in response if self.check_name(p['title'], checker)]            
            
            for d in response:
                out.append({"id": d["id"], "name": d["producer_granule_id"],
                    **{k: d[k] for k in ['links','collection_concept_id']+other_attrs}})
        
        log.info(f'{len(out)} products has been found')
        return Query(out)
    
    def download_file(self, product_id, dir):
        p = get_pattern(product_id)
        self.__init__(p['Name'], get_level(product_id, p))
        
        data = {'page_size': 5}
        headers = {'Accept': 'application/json'}
        url = 'https://cmr.earthdata.nasa.gov/search/granules'
        for collec in self.api_collection:   
            data['concept_id'] = collec
            data['granule_ur'] = product_id
            url_encode = url + '?' + urlencode(data)
            response = self.session.post(url_encode, headers=headers, verify=True)
            response = response.json()['feed']['entry']   
            if len(response) == 0: continue            
            
            dl_url = response[0]['links'][0]['href']
            target = Path(dir)/Path(dl_url).name
            filegen(if_exists='skip')(self._download)(target, dl_url)
            log.info(f'Product has been downloaded at : {target}')
            return target
        
        log.error(f'No file found with name {product_id}')
    
    
    def download(self, product: dict, dir: Path|str, if_exists='skip') -> Path:
        """
        Download a product from NASA data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
        """
        url = product['links'][0]['href']
        target = Path(dir)/Path(url).name
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
        meta = self.session.get(req).text

        assert len(meta) > 0
        with TemporaryDirectory() as tmpdir:
            with open(Path(tmpdir)/'meta.xml', 'w') as f:
                f.writelines(meta.split('\n'))
            return read_xml(Path(tmpdir)/'meta.xml')
    
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
