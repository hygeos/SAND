import pandas as pd


def Query(json_value: list[dict]) -> pd.DataFrame:
    """
    Convert a list of product dictionaries into a pandas DataFrame
    
    Args:
        json_value (list[dict]): List of dictionaries, each representing a product
            with keys like 'id', 'name', 'links', etc.
            
    Returns:
        pd.DataFrame: DataFrame containing all products, with each product as a row
            and dictionary keys as columns. Empty DataFrame if no products.
            The DataFrame is sorted by 'id'.
            
    Example:
        >>> products = [
        ...     {"id": "123", "name": "prod1", "links": {...}},
        ...     {"id": "456", "name": "prod2", "links": {...}}
        ... ]
        >>> df = Query(products)
    """
    # If no product returns empty pandas DataFrame
    if len(json_value) == 0: return pd.DataFrame()
    
    # Concatenate every product data into a DataFrame
    res = []
    for d in json_value:
        sub = pd.DataFrame.from_dict({k: [v] for k,v in d.items()})
        res.append(sub)
    
    # Sort query results by name
    df = pd.concat(res, ignore_index=True)
    df = df.sort_values(by='id')
    return df 

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