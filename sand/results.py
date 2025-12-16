import pandas as pd
from core.ascii_table import ascii_table
from dataclasses import dataclass, asdict


@dataclass
class SandProduct:
    product_id: str
    date: str
    metadata: dict
    index: str = None
    
    def to_dict(self):
        return asdict(self)

class SandQuery:
    """
    Format a list of product dictionaries.
    
    This class is fully serializable using pickle for saving and loading query results.
    
    Args:
        json_value (list[dict]): List of dictionaries, each representing a product
            with keys like 'id', 'name', 'links', etc.
    """
    
    def __init__(self, json_values: list[SandProduct]):
        self.products = json_values
    
    def __repr__(self):
        # If no product returns empty pandas DataFrame
        if len(self.products) == 0: 
            ascii_table(pd.DataFrame()).print()
        
        # Concatenate every product data into a DataFrame
        res = []
        for d in self.products:
            sub = pd.DataFrame.from_dict(d.to_dict())
            res.append(sub)
        
        # Sort query results by name
        df = pd.concat(res, ignore_index=True)
        df = df.sort_values(by='product_id')
        df = df.drop('metadata', axis=1)
        ascii_table(df).print()
        return ''
    
    def __len__(self):
        return len(self.products)
    
    def __getitem__(self, index):
        product = self.products[index]
        assert isinstance(product, SandProduct)
        return product
    
    def equals(self, obj):
        return self.products == obj.products

def Collection(selection: list[str], collec_table: pd.DataFrame) -> pd.DataFrame:
    """
    Extract a selection of collections from a reference table as pandas DataFrame
    
    This function filters a reference table of collections to include only the
    requested collections, verifying that all requested collections exist.

    Args:
        selection (list[str]): List of collection names to select
        collec_table (pd.DataFrame): Reference table containing collection information.
            Must have a 'Name' column containing collection names.

    Returns:
        pd.DataFrame: Filtered table containing only the selected collections,
            sorted by collection name

    Raises:
        AssertionError: If any requested collection name is not found in the reference table

    Example:
        >>> ref_table = pd.DataFrame({
        ...     'Name': ['LANDSAT-5-TM', 'SENTINEL-2', 'VENUS'],
        ...     'Level': [1, 2, 1]
        ... })
        >>> selected = Collection(['LANDSAT-5-TM', 'VENUS'], ref_table)
        >>> print(selected['Name'])
        0    LANDSAT-5-TM
        1    VENUS
    """
    assert all(c in collec_table['Name'].values for c in selection), \
    f'Some collection in {selection} does not exists in {collec_table["Name"]}'
    
    # Filter reference table
    filt = [c in selection for c in collec_table['Name']]
    data = collec_table.iloc[filt]
    data = data.sort_values(by='Name')
    data.reset_index(drop=True, inplace=True)
    
    return data