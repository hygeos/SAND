from core.table import read_csv, select
from pathlib import Path
from core import log
import re

def retrieve_product(product_id: str, fields: dict, pattern: tuple[str] = None) -> str:
    
    # Check fields input
    row = get_pattern(product_id).iloc[0]
    valid_fields = get_fields(row['pattern'])
    assert all(k in valid_fields for k in fields.keys()), \
    f'Invalid fields: keys of fields should be in {valid_fields}, got {fields.keys()}'
    
    # Transform giving fields
    retrieve = []
    rules = {valid_fields[i]: r for i,r in enumerate(row['regexp'].split(' '))}
    decompo = decompose(product_id, row['regexp'].split(' '))
    for i, field in enumerate(valid_fields):
        if field not in fields: retrieve.append(decompo[i])
        elif check(fields[field], rules[field]):
            retrieve.append(fields[field]) 
        else:
            log.error(f'Value for field {field} ({fields[field]}) '
                      f'does not satisfy regexp {rules[field]}', 
                      e=ValueError)
    
    # Join pieces to create new product id
    new = '_'.join(retrieve)
    ext = product_id.split('.')[1:]
    if len(ext) != 0: new = '.'.join([new]+ext)
    return new

def get_pattern(name: str) -> dict:
    
    # Find patterns
    db = read_csv(Path(__file__).parent/'patterns.csv')
    sensors = _find_pattern(name.split('.')[0], db)
    
    # Check patterns contentpattern
    if len(sensors) == 0: log.error(f'No match found for {name}', e=Exception)
    assert len(sensors) == 1, f'Multiple matches, got {sensors}'
    sensor = sensors[0]
    
    return select(db, ('Name','=',sensor))  

def _find_pattern(name: str, database) -> str:
    out = []
    for _, row in database.iterrows():
        regexp = row['regexp'].strip().split(' ')
        regexp = '_'.join(regexp)
        if check(name, regexp): out.append(row['Name'])
    return out

def check(name: str, regexp: str) -> bool:
    return bool(re.fullmatch(regexp, name))

def decompose(name: str, regexps: list[str], sep: str = '_') -> list:
    """
    Decompose a string according to the list of regexp, 
    assumming that every regexp block are splitted by sep
    """
    l = []
    seps = ['_' for i in range(len(regexps[:-1]))] + ['']
    for i in range(100): # Prefer using for loop rather than while loop
        if i == len(seps): break
        check = re.match(regexps[i] + seps[i], name)
        if check: 
            l.append(name[:check.span()[1]-len(seps[i])])
            name = name[check.span()[1]:]
        else: raise ValueError('name can be decompose be regexp list')
    return l

def get_fields(name: str, out: list = []):
    if len(name) == 0: return out
    check = re.match('[{_]*', name)
    name = name[check.span()[1]:]
    if check: 
        check = re.match('[0-9a-zA-Z_]*}', name)
        if not check: raise Exception
        out.append(name[:check.span()[1]-1])
        name = name[check.span()[1]:]
    return get_fields(name, out)