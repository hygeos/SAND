import eumdac
import requests
import shutil

from tqdm import tqdm
from pathlib import Path
from typing import Optional
from shapely import to_wkt
from tempfile import TemporaryDirectory
from datetime import datetime, time, date

from sand.base import UnauthorizedError, BaseDownload
from sand.results import Query, Collection
from core import log
from core.ftp import get_auth
from core.fileutils import filegen
from core.table import select_one, select, read_csv
from core.uncompress import uncompress as func_uncompress


class DownloadEumDAC(BaseDownload):
    
    name = 'DownloadEumDAC'
    
    collections = [
        'AMSU',
        'ASCAT-METOP-FR',
        'ASCAT-METOP-RES',
        'FCI-MTG-HR',
        'FCI-MTG-NR',
        'IASI', 
        'MVIRI-MFG',
        'SENTINEL-3-OLCI-FR',
        'SENTINEL-3-OLCI-RR',
        'SENTINEL-3-SRAL',
        'SENTINEL-5P-TROPOMI',
        'SEVIRI-MSG',
        'VIIRS',
    ]
    
    def __init__(self, collection: str = None, level: int = 1):
        """
        Python interface to the EuMetSat Data Access API Client (https://data.eumetsat.int/)

        Args:
            collection (str): collection name ('SENTINEL-2', 'SENTINEL-3', etc.)

        Example:
            eum = DownloadEumDAC('SENTINEL-2')
            # retrieve the list of products
            # using a json cache file to avoid reconnection
            ls = cache_json('query-S2.json')(eum.query)(
                dtstart=datetime(2024, 1, 1),
                dtend=datetime(2024, 2, 1),
                geo=Point(119.514442, -8.411750),
                name_contains=['_MSIL1C_'],
            )
            for p in ls:
                eum.download(p, <dirname>, uncompress=True)
        """
        self.available_collection = DownloadEumDAC.collections
        self.table_collection = Path(__file__).parent/'collections'/'eumdac.csv'
        super().__init__(collection, level)
        
    def _login(self):
        """
        Login to Eumetsat API with credentials storted in .netrc
        """
        auth = get_auth('data.eumetsat.int')
        
        credentials = (auth['user'], auth['password'])
        self.tokens = eumdac.AccessToken(credentials)
        try:
            if self.tokens.expiration < datetime.now():
                raise UnauthorizedError("Tokens has expired. Please refresh on https://api.eumetsat.int/api-key/#")
        except requests.exceptions.HTTPError:
            raise UnauthorizedError("Invalid Credentials")  
        
        self.datastore = eumdac.DataStore(self.tokens)        
        log.info(f'Log to API (https://data.eumetsat.int/)')

    def _check_collection(self):
        datastore = eumdac.DataStore(self.tokens)
        data = {c.title: c.abstract for c in datastore.collections}
        return Collection(data)

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
        use_most_recent: bool = True,
        other_attrs: Optional[list] = None,
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
            This method can be decorated by cache_json for storing the outputs.
            Example:
                cache_json('cache_result.json')(cds.query)(...)
        """
        # https://documentation.dataspace.copernicus.eu/APIs/OData.html#query-by-name
        if isinstance(dtstart, date):
            dtstart = datetime.combine(dtstart, time(0))
        if isinstance(dtend, date):
            dtend = datetime.combine(dtend, time(0))
        
        self.selected_collection = self.datastore.get_collection(self.collection)
        product = list(self.selected_collection.search(
            geo = to_wkt(geo),
            dtstart = dtstart,
            dtend = dtend
        ))
        
        # test if maximum number of returns is reached
        top = 1000  # maximum value of number of retrieved values
        if len(product) >= top:
            raise ValueError('The request led to the maximum number '
                             f'of results ({len(product)})')
        
        out = [{"id": str(d), 
                "name": d.acronym, 
                "collection": d.collection, 
                "time": d.processingTime,
                "dl_url": d.metadata['properties']['links']['data'],
                "meta_url": d.metadata['properties']['links']['alternates'],
                } 
                for d in product]
        return Query(out)

    def download(self, product: str, dir: Path, uncompress: bool=False) -> Path:
        """
        Download a product to directory

        product_id: 'S3A_OL_1_ERR____20231214T232432_20231215T000840_20231216T015921_2648_106_358______MAR_O_NT_002.SEN3'
        """
        data = self.datastore.get_product(
            product_id=product['id'],
            collection_id=self.collection,
        )

        @filegen()
        def _download(target: Path):
            with TemporaryDirectory() as tmpdir:
                target_compressed = Path(tmpdir)/(product['id'] + '.zip')
                with data.open() as fsrc, open(target_compressed, mode='wb') as fdst:
                    pbar = tqdm(total=data.size*1e3, unit_scale=True, unit="B",
                                initial=0, unit_divisor=1024, leave=False)
                    pbar.set_description(f"Downloading {product['id']}")
                    while True:
                        chunk = fsrc.read(1024)
                        if not chunk:
                            break
                        fdst.write(chunk)
                        pbar.update(len(chunk))
                log.info(f"Download of product {product['id']} finished.")
                if uncompress:
                    func_uncompress(target_compressed, target.parent)
                else:
                    shutil.move(target_compressed, target.parent)

        target = Path(dir)/(product['id'] if uncompress else (product['id'] + '.zip'))

        _download(target)

        return target
    
    def metadata(self, product):
        """
        Returns the product metadata including attributes and assets
        """
        
        req = (product['meta_url'][0]['href'])
        json = requests.get(req)

        assert len(json['value']) == 1
        return json['value'][0]
    
    def _retrieve_collec_name(self, collection):
        correspond = read_csv(self.table_collection)
        collecs = select(correspond,('level','=',self.level),['SAND_name','collec'])
        collecs = select_one(collecs,('SAND_name','=',collection),'collec')  
        return collecs.split(' ')[0]