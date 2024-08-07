import eumdac
import requests
import shutil

from tqdm import tqdm
from pathlib import Path
from typing import Optional
from shapely import to_wkt
from tempfile import TemporaryDirectory
from datetime import datetime, time, date

from sandd.base import UnauthorizedError
from core.uncompress import uncompress as func_uncompress
from core.ftp import get_auth
from core.fileutils import filegen


class DownloadEumDAC:
    
    name = 'DownloadEumDAC'
    
    collections = [
        'MSG-SEVIRI',
        'MTG-FCI',
    ]
    
    def __init__(self, collection: str, level: int = 1):
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
        assert collection in DownloadEumDAC.collections
        self.collection = collection
        self.level = level
        self._login()
        
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
        print(f'Log to API (https://data.eumetsat.int/)')


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
        
        self.eumdac_collection = self._get_dataset_name()
        self.selected_collection = self.datastore.get_collection(self.eumdac_collection)
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
        
        return [str(d) for d in product]

    def download(self, product_id: str, dir: Path, uncompress: bool=False) -> Path:
        """
        Download a product to directory

        product_id: 'S3A_OL_1_ERR____20231214T232432_20231215T000840_20231216T015921_2648_106_358______MAR_O_NT_002.SEN3'
        """
        product = self.datastore.get_product(
            product_id=product_id,
            collection_id=self.eumdac_collection,
        )

        @filegen()
        def _download(target: Path):
            with TemporaryDirectory() as tmpdir:
                target_compressed = Path(tmpdir)/(product_id + '.zip')
                with product.open() as fsrc, open(target_compressed, mode='wb') as fdst:
                    pbar = tqdm(total=product.size*1e3, unit_scale=True, unit="B",
                                initial=0, unit_divisor=1024, leave=False)
                    pbar.set_description(f"Downloading {product_id}")
                    while True:
                        chunk = fsrc.read(1024)
                        if not chunk:
                            break
                        fdst.write(chunk)
                        pbar.update(len(chunk))
                print(f'Download of product {product} finished.')
                if uncompress:
                    func_uncompress(target_compressed, target.parent)
                else:
                    shutil.move(target_compressed, target.parent)

        target = Path(dir)/(product_id if uncompress else (product_id + '.zip'))

        _download(target)

        return target
    
    def _get_dataset_name(self):
        if self.collection == 'MSG-SEVIRI':
            collec_name = 'EO:EUM:DAT:MSG:HRSEVIRI'
        if self.collection == 'EUMET-RSS':
            collec_name = 'EO:EUM:DAT:MSG:MSG15-RSS'
        if self.collection == 'EUMET-OLCI-FR':
            collec_name = ['EO:EUM:DAT:0409', 'EO:EUM:DAT:0577']
        if self.collection == 'EUMET-OLCI-RR':
            collec_name = ['EO:EUM:DAT:0410', 'EO:EUM:DAT:0578']
        if self.collection == 'MTG-FCI':
            collec_name = ''
        
        return collec_name