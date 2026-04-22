from sand.results import SandQuery, SandProduct, cache_sandquery
from tempfile import TemporaryDirectory
from pathlib import Path


def test_sandquery_indexing():
    """Test single item indexing"""
    products = [
        SandProduct(product_id=f"product_{i%10}", date="2024-01-01", metadata={}, index=str(i%10))
        for i in range(5,15)
    ]
    query = SandQuery(products)
    
    # Test single indexing
    assert query[0].product_id == "product_0"
    assert query[5].product_id == "product_5"
    assert query[-1].product_id == "product_9"
    assert isinstance(query[0], SandProduct)


def test_sandquery_slicing():
    """Test slicing functionality"""
    products = [
        SandProduct(product_id=f"product_{i}", date="2024-01-01", metadata={}, index=str(i))
        for i in range(10)
    ]
    query = SandQuery(products)
    
    # Test slice returns SandQuery
    sliced = query[:4]
    assert isinstance(sliced, SandQuery)
    assert len(sliced) == 4
    assert sliced[0].product_id == "product_0"
    assert sliced[3].product_id == "product_3"
    
    # Test various slice patterns
    assert len(query[2:5]) == 3
    assert query[2:5][0].product_id == "product_2"
    assert query[2:5][2].product_id == "product_4"
    
    assert len(query[5:]) == 5
    assert query[5:][0].product_id == "product_5"
    
    assert len(query[:3]) == 3
    assert len(query[::2]) == 5  # Every other item
    assert len(query[-3:]) == 3  # Last 3 items


def test_sandquery_iteration():
    """Test that SandQuery is iterable"""
    products = [
        SandProduct(product_id=f"product_{i}", date="2024-01-01", metadata={}, index=str(i))
        for i in range(5)
    ]
    query = SandQuery(products)
    
    # Test iteration
    product_ids = [p.product_id for p in query]
    assert product_ids == ["product_0", "product_1", "product_2", "product_3", "product_4"]
    
    # Test in for loop
    count = 0
    for product in query:
        assert isinstance(product, SandProduct)
        count += 1
    assert count == 5


def test_sandquery_cache_property():
    """Test that SandQuery is compatible with cache_json"""
    
    def fake_query():
        products = [
            SandProduct(product_id=f"product_{i%10}", date="2024-01-01", metadata={}, index=str(i%10))
            for i in range(5,15)
        ]
        return SandQuery(products)
    
    with TemporaryDirectory() as tmpdir:
        path = Path(tmpdir)/'test.json'
        cache_sandquery(path)(fake_query)()
        assert path.exists()
        