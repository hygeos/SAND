import pandas as pd


def Query(json_value: dict) -> pd.DataFrame:
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

def Collection(selection: list, collec_table: pd.DataFrame) -> pd.DataFrame:
    """
    Function to extract a selection of collection from a reference table as pandas DataFrame

    Args:
        selection (list): List of selected collections
        collec_table (pd.DataFrame): Reference table

    Returns:
        pd.DataFrame: Filtered table containing selected collections
    """
    assert all(c in collec_table['Name'].values for c in selection), \
    f'Some collection in {selection} does not exists in {collec_table['Name']}'
    
    # Filter reference table
    filt = [c in selection for c in collec_table['Name']]
    data = collec_table.iloc[filt]
    data = data.sort_values(by='Name')
    data.reset_index(drop=True, inplace=True)
    
    return data