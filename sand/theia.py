import requests

from datetime import datetime, date
from pathlib import Path
from typing import Optional

from sand.base import BaseDownload, raise_api_error
from sand.results import Query
from sand.tinyfunc import *
from core import log
from core.ftp import get_auth
from core.static import interface
from core.fileutils import filegen
from core.table import select_cell, select


# [SOURCE] https://github.com/olivierhagolle/theia_download/tree/master
class DownloadTHEIA(BaseDownload):
    
    name = 'DownloadTHEIA'
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the CNES Theia Data Center (https://theia.cnes.fr/)

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
        self.provider = 'theia'
        super().__init__(collection, level)

    def _login(self):
        """
        Login to copernicus dataspace with credentials storted in .netrc
        """
        auth = get_auth("theia.cnes.fr")     
        
        data = {
            "ident" : auth['user'],
            "pass"  : auth['password'],
            }
        
        try:
            url = 'https://theia.cnes.fr/atdistrib/services/authenticate/'
            r = requests.post(url, data=data)
            r.raise_for_status()
        except Exception:
            raise Exception(
                f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
                )
        self.tokens = r.text
        log.info('Log to API (https://theia.cnes.fr/)')
    
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
    ):
        """
        Product query on the Theia Data Center

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
            
        query_lines = [
            f"https://theia.cnes.fr/atdistrib/resto2/api/collections/{self.collection}/search.json?"
        ]

        if dtstart:
            query_lines.append(f"startDate={dtstart.strftime('%Y-%m-%d')}")
        if dtend:
            query_lines.append(f"completionDate={dtend.strftime('%Y-%m-%d')}")
        
        assert sum(v is not None for v in [geo, tile_number, venus_site]) != 0, \
        "Please fill in at least geo or tile number or venus site"
        assert sum(v is not None for v in [geo, tile_number, venus_site]) == 1
        if geo:
            query_lines.append(f"q={geo}")
        if tile_number:
            query_lines.append(f"location={tile_number}")
        if venus_site:
            query_lines.append(f"location={venus_site}")

        if cloudcover_thres:
            query_lines.append(f"maxcloud={cloudcover_thres}")

        top = 500  # maximum value of number of retrieved values
        req = ('&'.join(query_lines))+f'&maxRecords={top}'
        response = requests.get(req, verify=True)
        raise_api_error(response)
        
        # Filter products
        r = response.json()['features']
        response = [p for p in r if self.check_name(p["properties"]['title'], checker)]   

        # test if maximum number of returns is reached
        if len(response) >= top:
            log.error('The request led to the maximum number of results '
                      f'({len(response)})', e=ValueError)
        else: log.info(f'{len(response)} products has been found')

        out =  [{"id": d["id"], "name": d["properties"]["productIdentifier"],
                 **{k: d['properties'][k] for k in (other_attrs or ['quicklook','startDate', 'links','services'])}}
                for d in response]
        
        return Query(out)
    
    @interface
    def download(self, product: dict, dir: Path|str, if_exists='skip', uncompress: bool=True) -> Path:
        """
        Download a product from Theia Datahub

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
            uncompress (bool, optional): If True, uncompress file if needed. Defaults to True.
        """
        target = Path(dir)/(product['name'])
        url = product['services']['download']['url']
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
        content = requests.get(url).content
        with open(target, 'wb') as f:
            f.write(content)
    
    @interface 
    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        target = Path(dir)/(product['name'] + '.jpeg')
        url = product['quicklook']

        if not target.exists():
            filegen(0)(self._download)(target, url)
        
        log.info(f'Quicklook has been downloaded at : {target}')
        return target
        
    @interface           
    def metadata(self, product: dict):
        """
        Returns the product metadata including attributes and assets
        """
        req = self._get(product['links'], 'Metadata', 'title', 'href')
        meta = requests.get(req).text
        
        if 'Request Rejected' in meta:  raise FileNotFoundError
        return meta
    
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