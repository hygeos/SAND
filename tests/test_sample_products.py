from sand.sample_product import products
from core.table import read_csv
from pathlib import Path


def test_product_collection_name():
    ref_file = Path(__file__).parent.parent/'sand'/'sensors.csv'
    sensors = read_csv(ref_file)['Name'].values
    collecs = list(products)
    difference = set(sensors).difference(collecs).intersection(collecs)
    assert len(difference) == 0, f'Following collections in sample product are wrong: {difference}'