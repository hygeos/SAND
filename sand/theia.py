import requests
import json

from urllib.request import urlopen, Request
from datetime import datetime, date, time
from pathlib import Path
from typing import Optional
from tqdm import tqdm

from sand.base import request_get, BaseDownload
from sand.results import Query
from sand.tinyfunc import *
from core import log
from core.ftp import get_auth
from core.fileutils import filegen
from core.table import select_cell, select, read_csv, read_xml_from_text


# [SOURCE] https://github.com/olivierhagolle/theia_download/tree/master
class DownloadTHEIA(BaseDownload):
    
    name = 'DownloadTHEIA'
    
    collections = [
        'LANDSAT-5-TM',
        'LANDSAT-7-ET', 
        'LANDSAT-8-OLI', 
        'PLEIADES',
        'SPOT', 
        'SWH', 
        'SENTINEL-2-MSI', 
        'VENUS', 
        'VENUS-VM5',
    ]
    
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
        self.available_collection = DownloadTHEIA.collections
        self.table_collection = Path(__file__).parent/'collections'/'theia.csv'
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
        if isinstance(dtstart, date):
            dtstart = datetime.combine(dtstart, time(0))
        if isinstance(dtend, date):
            dtend = datetime.combine(dtend, time(0))
        
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
        urllib_req = Request(req)
        urllib_response = urlopen(urllib_req, timeout=5, context=self.ssl_ctx)
        response = json.load(urllib_response)['features']
        
        # Filter products
        response = [p for p in response if self.check_name(p["properties"]['title'], checker)]   

        # test if maximum number of returns is reached
        if len(response) >= top:
            raise ValueError('The request led to the maximum number '
                             f'of results ({len(response)})')

        out =  [{"id": d["id"], "name": d["properties"]["productIdentifier"],
                 **{k: d['properties'][k] for k in (other_attrs or ['quicklook','startDate', 'links','services'])}}
                for d in response]
        
        return Query(out)

    def download(self, product: dict, dir: Path|str, uncompress: bool=True) -> Path:
        """
        Download a product from Theia Datahub

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
            uncompress (bool, optional): If True, uncompress file if needed. Defaults to True.
        """
        if uncompress:
            target = Path(dir)/(product['name'])
            uncompress_ext = '.zip'
        else:
            target = Path(dir)/(product['name']+'.zip')
            uncompress_ext = None

        url = product['services']['download']['url']

        filegen(0, uncompress=uncompress_ext)(self._download)(target, url)

        return target

    def quicklook(self, product: dict, dir: Path|str):
        """
        Download a quicklook to `dir`
        """
        target = Path(dir)/(product['name'] + '.jpeg')
        url = product['quicklook']

        if not target.exists():
            filegen(0)(self._download)(target, url)
        
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
                    
    def metadata(self, product: dict):
        """
        Returns the product metadata including attributes and assets
        """
        req = self._get(product['links'], 'Metadata', 'title', 'href')
        meta = requests.get(req).text
        
        if 'Request Rejected' in meta:  raise FileNotFoundError
        return meta
    
    def _retrieve_collec_name(self, collection):
        correspond = read_csv(self.table_collection)
        collecs = select(correspond,('level','=',self.level),['SAND_name','collec'])
        collecs = select_cell(collecs,('SAND_name','=',collection),'collec')  
        return collecs.split(' ')[0]
    
    def _get(self, liste, name, in_key, out_key):
        for col in liste:
            if name in col[in_key]:
                print(col[in_key])
                return col[out_key]
        raise FileNotFoundError