import pytest

from sand.sample_product import products
from core.geo.product_name import get_pattern, retrieve_product


list_prod = []
for name, dico in products.items():
    for level, params in dico.items():
        if 'product_id' in params:
            list_prod.append((name, params['product_id']))

@pytest.fixture(params=list_prod)
def product(request): return request.param

def test_identify_product(product):
    sensor, example_prod = product
    assert sensor == get_pattern(example_prod)['Name']

@pytest.mark.parametrize('example_prod', ['LC08_L1GT_029030_20151209_20160131_01_RT'])
def test_retrieve_product(example_prod):
    pattern = get_pattern(example_prod)
    print(retrieve_product(example_prod, {'level': 'L2GS'}, pattern))