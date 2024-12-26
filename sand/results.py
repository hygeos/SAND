import pandas as pd


def Query(json_value: dict) -> None:
    if len(json_value) == 0:
        return pd.DataFrame()
    res = []
    for d in json_value:
        sub = pd.DataFrame.from_dict({k: [v] for k,v in d.items()})
        res.append(sub)
    return pd.concat(res, ignore_index=True) 

def Collection(json_value: dict):
    data = dict(Name=json_value.keys(), Description=json_value.values())
    data = pd.DataFrame(data).sort_values(by='Name')
    data.reset_index(drop=True, inplace=True)
    return data.style.set_properties(**{'text-align': 'left'})