from datetime import datetime, date
from shapely import Point
from pathlib import Path
from typing import Optional

from sand.base import BaseDownload, raise_api_error, check_too_many_matches
from sand.results import Query
from sand.tinyfunc import (
    check_name_contains, 
    check_name_glob,
    check_name_endswith,
    check_name_startswith,
)

from core import log
from core.network.auth import get_auth
from core.files.fileutils import filegen
from core.table import select_cell, select

import re


# [SOURCE] https://github.com/olivierhagolle/theia_download/tree/master
# https://geodes.cnes.fr/support/api/
class DownloadCNES(BaseDownload):
    
    name = 'DownloadCNES'
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the CNES Geodes Data Center (https://geodes-portal.cnes.fr/)

        Args:
            collection (str): collection name ('LANDSAT-5-TM', 'VENUS', etc.)

        Example:
            cds = DownloadCNES('VENUS')
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
            )
            cds.download(ls.iloc[0], <dirname>)
        """
        self.provider = 'geodes'
        super().__init__(collection, level)

    def _login(self):
        """
        Login to copernicus dataspace with credentials storted in .netrc
        """
        auth = get_auth("geodes.cnes.fr")     
        self.tokens = auth['password']
        log.debug('Log to API (https://geodes-portal.cnes.fr/)')
    
    
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
        tile_number: str = None,
        venus_site: str = None,
        other_attrs: Optional[list] = [],
        **kwargs
    ):
        """
        Product query on the Geodes Data Center

        Args:
            dtstart and dtend (datetime): start and stop datetimes
            geo: shapely geometry with 0<=lon<360 and -90<=lat<90. Examples:
                Point(lon, lat)
                Polygon(...)
            cloudcover_thres: Optional[int]=None, 
            name_contains (list): list of substrings
            name_startswith (str): search for name starting with this str
            name_endswith (str): search for name ending with this str
            name_glob (str): match name with this string
            use_most_recent (bool): keep only the most recent processing baseline version
            tile_number (str): Tile number (ex: T31TCJ), Sentinel2 only
            venus_site (str): Venµs Site name, Venµs only
            other_attrs (list): list of other attributes to include in the output
                (ex: ['ContentDate', 'Footprint'])

        Note:
            This method can be decorated by cache_dataframe for storing the outputs.
            Example:
                cache_dataframe('cache_result.pickle')(cds.query)(...)
        """
        dtstart, dtend, geo = self._format_input_query(dtstart, dtend, geo)
        
        # Add provider constraint
        name_contains = self._complete_name_contains(name_contains)
        
        # Define check functions
        checker = []
        if name_contains: checker.append((check_name_contains, name_contains))
        if name_startswith: checker.append((check_name_startswith, name_startswith))
        if name_endswith: checker.append((check_name_endswith, name_endswith))
        if name_glob: checker.append((check_name_glob, name_glob))
        
        server_url = "https://geodes-portal.cnes.fr/api/stac/search"
        data = {'page':1, 'limit':500}
        query = {'dataset': {'in': [self.api_collection]}}

        if dtstart:
            query['start_datetime'] = {'gte':dtstart.isoformat()+'Z'}
        if dtend:
            query['end_datetime'] = {'lte':dtend.isoformat()+'Z'}
        
        log.check(sum(v is not None for v in [geo, tile_number, venus_site])<2)
        if isinstance(geo, Point): geo = geo.buffer(0.5)
        if geo: data['bbox'] = list(geo.bounds)
        if tile_number: query["location"] = tile_number
        if venus_site: query["grid:code"] = {'contains': venus_site}
        if cloudcover_thres: query['eo:cloud_cover'] = {"lte":cloudcover_thres}
        
        data['query'] = query
        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        response = self.session.post(server_url, json=data, verify=True)
        raise_api_error(response)
        
        # Filter products
        check_too_many_matches(response.json(), ['context','returned'], ['context','matched'])
        r = response.json()['features']
        response = [p for p in r if self.check_name(p["properties"]['identifier'], checker)]   

        out =  [{"id": d["id"], "name": d["properties"]["identifier"], 
                 'links': d['assets'], 'time': d['properties']['datetime'],
                 **{k: d[k] for k in other_attrs}}
                 for d in response]
        
        log.info(f'{len(out)} products has been found')
        return Query(out)

    def download(self, product: dict, dir: Path|str, if_exists='skip') -> Path:
        """
        Download a product from Geodes Datahub

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
        """
        search = [l for l in product['links'] if re.search(product['name']+'.*.zip',l)]
        log.check(len(search) == 1, "No download link for product found")
        target = Path(dir)/search[0].replace('.zip','')
        dl_data = product['links'][search[0]]
        filegen(0, if_exists=if_exists, uncompress='.zip')(self._download)(target, dl_data)
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
        desc = url['description'].split()
        filesize = int(desc[desc.index('bytes')-1])
        self.session.headers.update({"X-API-Key": self.tokens})
        response = self.session.get(url['href'], verify=True)
        pbar = log.pbar(log.lvl.INFO, total=filesize, unit_scale=True, unit="B", 
                        desc='writing', unit_divisor=1024, leave=False)
        raise_api_error(response)
        with open(target, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(1024)
    
    def download_file(self, product_id: str, dir, if_exists='skip'):
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
        out =  [{"id": d["id"], "name": d["properties"]["identifier"], 
                 'links': d['assets'], 'time': d['properties']['datetime']}
                for d in r]
        return self.download(Query(out).iloc[0], dir, if_exists)

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        search = [l for l in product['links'] if 'quicklook' in l]
        log.check(len(search) == 1, "No download link for quicklook found")
        target = Path(dir)/search[0].split('/')[-1]
        url = product['links'][search[0]]

        if not target.exists():
            filegen(0)(self._download)(target, url)
        
        log.info(f'Quicklook has been downloaded at : {target}')
        return target
          
    def metadata(self, product: dict):
        """
        Returns the product metadata including attributes and assets
        """
        raise NotImplementedError
    
    def _retrieve_collec_name(self, collection):
        collecs = select(self.provider_prop,('SAND_name','=',collection),['level','collec'])
        try: collecs = select_cell(collecs,('level','=',self.level),'collec')
        except AssertionError: log.error(
            f'Level{self.level} products are not available for {self.collection}', e=KeyError)
        return collecs.split(' ')[0]
    
    def _get(self, liste, name, in_key, out_key):
        for col in liste:
            if in_key not in col: continue
            if name in col[in_key]:
                return col[out_key]
        log.error(f'{name} has not been found', e=KeyError)
