from datetime import datetime, date, time
from requests.utils import requote_uri
from urllib.parse import urlencode
from pathlib import Path
from typing import Optional
from typing import Literal

import re
import fnmatch
import requests

from sand.tinyfunc import _parse_geometry, change_lon_convention
from sand.base import raise_api_error, BaseDownload
from sand.results import Query

from core import log
from core.ftp import get_auth
from core.fileutils import filegen
from core.table import select_cell, select
from core.static import interface


class DownloadCDSE(BaseDownload):
    
    name = 'DownloadCDSE'
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the Copernicus Data Space (https://dataspace.copernicus.eu/)

        Args:
            collection (str): collection name ('SENTINEL-2-MSI', 'SENTINEL-3-OLCI', etc.)

        Example:
            cds = DownloadCDS('SENTINEL-2-MSI')
            # retrieve the list of products
            # using a pickle cache file to avoid reconnection
            ls = cache_dataframe('query-S2.pickle')(cds.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
                name_contains=['_MSIL1C_'],
            )
            cds.download(ls.iloc[0], <dirname>, uncompress=True)
        """
        self.api = 'OData'
        self.provider = 'cdse'
        super().__init__(collection, level)

    def _login(self):
        """
        Login to copernicus dataspace with credentials storted in .netrc
        """
        auth = get_auth("dataspace.copernicus.eu")

        data = {
            "client_id": "cdse-public",
            "username": auth['user'],
            "password": auth['password'],
            "grant_type": "password",
            }
        try:
            url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
            r = requests.post(url, data=data)
            r.raise_for_status()
        except Exception:
            raise Exception(
                f"Keycloak token creation failed. Reponse from the server was: {r.json()}"
                )
        self.tokens = r.json()["access_token"]
        log.info('Log to API (https://dataspace.copernicus.eu/)')
    
    @interface
    def change_api(self, api_name: Literal['OData', 'OpenSearch']):
        """
        To change backend Copernicus API
        """
        assert api_name in ['OData', 'OpenSearch']
        log.debug(f'Move from {self.api} API to {api_name} API')
        self.api = api_name
    
    @interface
    def query(
        self,
        dtstart: Optional[date|datetime] = None,
        dtend: Optional[date|datetime] = None,
        geo = None,
        cloudcover_thres: Optional[int] = None,
        name_contains: Optional[list] = [],
        name_startswith: Optional[str] = None,
        name_endswith: Optional[str] = None,
        name_glob: Optional[str] = None,
        use_most_recent: bool = True,
        other_attrs: Optional[list] = None,
        **kwargs
    ):
        """
        Product query on the Copernicus Data Space

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
        # https://documentation.dataspace.copernicus.eu/APIs/OData.html#query-by-name
        dtstart, dtend, geo = self._format_input_query(dtstart, dtend, geo)
        geo = change_lon_convention(geo)
        
        # Add provider constraint
        name_contains = self._complete_name_contains(name_contains)
        
        log.debug(f'Query {self.api} API')
        params = (dtstart, dtend, geo, name_glob, name_contains, name_startswith, name_endswith, cloudcover_thres)
        if self.api == 'OpenSearch':
            response = self._query_opensearch(*params)
        elif self.api == 'OData': 
            response = self._query_odata(*params)
        else: log.error(f'Invalid API, got {self.api}', e=ValueError)

        # test if maximum number of returns is reached
        if len(response) >= 1000:
            log.error('The request led to the maximum number of results '
                      f'({len(response)})', e=ValueError)
        else: log.info(f'{len(response)} products has been found')
        
        if use_most_recent and (self.api_collection == 'SENTINEL-2'):
            # remove duplicate products, take only the most recent one
            mp = {}  # maps a single_id to a list of lines
            for args in response:
                # build a common identifier for multiple versions
                s = args['Name'].split('_')
                ident = '_'.join([s[i] for i in [0, 1, 2, 4, 5]])
                if ident in mp: mp[ident].append(args)
                else: mp[ident] = [args]
            # for each identifier, sort the corresponding lines by "Name"
            # and select the last one
            json_value = [sorted(lines, key=lambda line: line['Name'])[-1]
                             for lines in mp.values()]
        else: json_value = response

        out = [{"id": d["Id"], "name": d["Name"],
                 **{k: d[k] for k in (other_attrs or [])}}
                for d in json_value
                if ((not name_glob) or fnmatch.fnmatch(d["Name"], name_glob))
                ]
    
        return Query(out)
    
    @interface
    def download(self, product: dict, dir: Path|str, if_exists: str='skip', uncompress: bool=True) -> Path:
        """
        Download a product from copernicus data space

        Args:
            product (dict): product definition with keys 'id' and 'name'
            dir (Path | str): Directory where to store downloaded file.
            uncompress (bool, optional): If True, uncompress file if needed. Defaults to True.
        """        
        
        target = Path(dir)/(product['name'])
        url = ("https://catalogue.dataspace.copernicus.eu/odata/v1/"
               f"Products({product['id']})/$value")
        filegen(0, if_exists=if_exists, uncompress='.zip')(self._download)(target, url)
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
        status = False
        exp_timeout_cnt = 1
        
        while not status:
            
            if exp_timeout_cnt >= 16:
                log.error("The server error bypass method failed", e=RuntimeError)
                
            try:
                # Initialize session for download
                self.session.headers.update({'Authorization': f'Bearer {self.tokens}'})

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
                status = True
            
            except Exception as e: # exponential wait when ecountering errors
                log.warning(log.rgb.red, str(e))
                
                exp_timeout_cnt *= 2
                self.session.close()
                
                log.warning(log.rgb.red, f"Waiting {exp_timeout_cnt}min ...")
                time.sleep(60 * exp_timeout_cnt)
                
                self.session = requests.Session()
                
        # end while
        
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

        if not target.exists():
            assets = self.metadata(product)['Assets']
            if not assets:
                raise FileNotFoundError(f'Skipping quicklook {target.name}')
            url = assets[0]['DownloadLink']
            filegen(0, if_exists='skip')(self._download)(target, url)

        log.info(f'Quicklook has been downloaded at : {target}')
        return target
    
    @interface
    def metadata(self, product):
        """
        Returns the product metadata including attributes and assets
        """
        req = ("https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Id"
               f" eq '{product['id']}'&$expand=Attributes&$expand=Assets")
        json = requests.get(req).json()

        assert len(json['value']) == 1
        return json['value'][0]
    
    def _retrieve_collec_name(self, collection):
        collecs = select(self.provider_prop,('SAND_name','=',collection),['level','collec'])
        try: return select_cell(collecs,('level','=',self.level),'collec')
        except AssertionError: log.error(
            f'Level{self.level} products are not available for {self.collection}', e=KeyError)

    def _query_odata(self, dtstart, dtend, geo, name_glob, name_contains, name_startswith, name_endswith, cloudcover_thres):
        """Query the EOData Finder API"""
        
        query_lines = [
            f"""https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter=Collection/Name eq '{self.api_collection}' """
        ]

        if dtstart:
            query_lines.append(f'ContentDate/Start gt {dtstart.isoformat()}Z')
        if dtend:
            query_lines.append(f'ContentDate/Start lt {dtend.isoformat()}Z')
        if geo:
            query_lines.append(f"OData.CSC.Intersects(area=geography'SRID=4326;{geo}')")

        if name_glob:
            assert name_startswith is None
            assert name_endswith is None
            assert name_contains is None
            substrings = re.split(r'\*|\?', name_glob)
            if substrings[0]:
                name_startswith = substrings[0]
            if substrings[-1] and (len(substrings) > 1):
                name_endswith = substrings[-1]
            if (len(substrings) > 2):
                name_contains = [x for x in substrings[1:-1] if x]

        if name_startswith:
            query_lines.append(f"startswith(Name, '{name_startswith}')")

        if name_contains:
            assert isinstance(name_contains, list)
            for cont in name_contains:
                query_lines.append(f"contains(Name, '{cont}')")

        if name_endswith:
            query_lines.append(f"endswith(Name, '{name_endswith}')")

        if cloudcover_thres:
            query_lines.append(
                "Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq 'cloudCover' "
                f"and att/OData.CSC.DoubleAttribute/Value le {cloudcover_thres})")

        top = 1000  # maximum value of number of retrieved values
        req = (' and '.join(query_lines))+f'&$top={top}'
        # urllib_req = Request(requote_uri(req))
        # urllib_response = urlopen(urllib_req, timeout=5, context=self.ssl_ctx)
        # response = json.load(urllib_response)
        response = requests.get(requote_uri(req), verify=True)
        
        # assert urllib_response.status == 200
        return response.json()['value']

    def _query_opensearch(self, dtstart, dtend, geo, name_glob, name_contains, name_startswith, name_endswith, cloudcover_thres):
        """Query the OpenSearch Finder API"""
        
        def _get_next_page(links):
            for link in links:
                if link["rel"] == "next":
                    return link["href"]
            return False

        query = f"""https://catalogue.dataspace.copernicus.eu/resto/api/collections/{self.api_collection}/search.json?maxRecords=1000"""
        
        query_params = {'status': 'ALL'}
        if dtstart is not None: query_params["startDate"] = dtstart.isoformat()
        if dtend is not None: query_params["completionDate"] = dtend.isoformat()
        if geo is not None: query_params["geometry"] = _parse_geometry(geo)

        query += f"&{urlencode(query_params)}"
        
        query_response = []
        while query:
            response = requests.get(query, verify=True)
            response.raise_for_status()
            data = response.json()
            for feature in data["features"]:
                query_response.append(feature)
            query = _get_next_page(data["properties"]["links"])
        return query_response