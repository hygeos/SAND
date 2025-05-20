from datetime import datetime, date
from pathlib import Path
from typing import Optional

from sand.base import BaseDownload, raise_api_error
from sand.results import Query
from sand.tinyfunc import *
from core import log
from core.download.ftp import get_auth
from core.static import interface
from core.files.fileutils import filegen
from core.table import select_cell, select


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
            cds.download(ls.iloc[0], <dirname>, uncompress=True)
        """
        self.provider = 'geodes'
        super().__init__(collection, level)

    def _login(self):
        """
        Login to copernicus dataspace with credentials storted in .netrc
        """
        auth = get_auth("geodes.cnes.fr")     
        self.tokens = auth['password']
        log.info('Log to API (https://geodes-portal.cnes.fr/)')
    
    @interface
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
        other_attrs: Optional[list] = None,
        **kwargs
    ):
        """
        Product query on the Geodes Data Center

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
        
        server_url = f'https://geodes-portal.cnes.fr/api/stac/collections/{self.api_collection}/items'
        data = {'page':1, 'limit':500}
        query = {}

        if dtstart:
            query['datetime'] = {'lte':dtstart.isoformat()}
        if dtend:
            query['datetime']['gte'] = dtend.isoformat()
        
        assert sum(v is not None for v in [geo, tile_number, venus_site]) != 0, \
        "Please fill in at least geo or tile number or venus site"
        assert sum(v is not None for v in [geo, tile_number, venus_site]) == 1
        if geo: data['bbox'] = geo
        # if tile_number:
        #     query_lines.append(f"location={tile_number}")
        # if venus_site:
        #     query_lines.append(f"location={venus_site}")

        if cloudcover_thres: query['eo:cloud_cover'] = {"lte":cloudcover_thres}
        
        data['query'] = query
        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        response = self.session.get(server_url, data=data, verify=False)
        raise_api_error(response)
        
        # Filter products
        r = response.json()['features']
        response = [p for p in r if self.check_name(p["properties"]['identifier'], checker)]   

        # test if maximum number of returns is reached
        if len(response) >= 500:
            log.error('The request led to the maximum number of results '
                      f'({len(response)})', e=ValueError)
        else: log.info(f'{len(response)} products has been found')

        out =  [{"id": d["id"], "name": d["properties"]["identifier"], 
                 'links': d['assets'], 'time': d['properties']['datetime']}
                for d in response]
        
        return Query(out)
    
    @interface
    def download(self, product: dict, dir: Path|str, if_exists='skip', uncompress: bool=True) -> Path:
        """
        Download a product from Geodes Datahub

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
            uncompress (bool, optional): If True, uncompress file if needed. Defaults to True.
        """
        search = [l for l in product['links'] if product['name']+'.' in l]
        assert len(search) == 1
        target = Path(dir)/search[0]
        dl_data = product['links'][search[0]]
        filegen(0, if_exists=if_exists)(self._download)(target, dl_data)
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
        response = self.session.get(url['href'], verify=False)
        pbar = log.pbar(log.lvl.INFO, total=filesize, unit_scale=True, unit="B", 
                        desc='writing', unit_divisor=1024, leave=False)
        with open(target, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    pbar.update(1024)
    
    @interface
    def download_file(self, product_id: str, dir, if_exists='skip'):
        # Query and check if product exists
        server_url = f'https://geodes-portal.cnes.fr/api/stac/search'
        data = {'page':1, 'limit':1}
        data['query'] = {'identifier': {'contains':[product_id]}}
        self.session.headers.update({"X-API-Key": self.tokens})
        self.session.headers.update({"Content-type": "application/json"})
        
        response = self.session.get(server_url, data=data, verify=False)
        raise_api_error(response)        
        r = response.json()['features']
        log.check(len(r) > 0, f'No product named {product_id}')
        
        # Download the product
        out =  [{"id": d["id"], "name": d["properties"]["identifier"], 
                 'links': d['assets'], 'time': d['properties']['datetime']}
                for d in r]
        return self.download(Query(out).iloc[0], dir, if_exists)
        
    @interface 
    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        search = [l for l in product['links'] if 'quicklook' in l]
        assert len(search) == 1
        target = Path(dir)/search[0].split('/')[-1]
        url = product['links'][search[0]]

        if not target.exists():
            filegen(0)(self._download)(target, url)
        
        log.info(f'Quicklook has been downloaded at : {target}')
        return target
        
    @interface           
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