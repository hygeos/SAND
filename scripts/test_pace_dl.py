from sand.nasa import DownloadNASA
from datetime import date

dl = DownloadNASA()

ls = dl.query(
    collection_sand="PACE-OCI",
    dtstart=date(2025,1,1), 
    dtend=date(2025,1,1), 
    name_contains=['L1B'],
)

p = ls.iloc[0]
dl.download_file(p['name'], '.', api_collections=[p['collection_concept_id']])
pass