# MOAT API

Full documentation of MOAT API is available at http://api.moat.com/docs 
(after logging in with credentials)

## Preparation

To use MOAT API you'll need MOAT token set up in config file in `moat` part.

Additionally you need to define database id in config file in `moat_db` part
in a form `name: ID`.


## Methods

### `get_data_from_moat(moat_dict, database_name)`

Function that download data from MOAT db through API to pandas DataFrame.

#### Arguments
```
Args:
    moat_dict (dict):  dictionary with keys: 'start' (str : str) (start date of analysis 'YYYYMMDD') - obligatory,
                                             'end' (str : str) (end date of analysis 'YYYYMMDD') - obligatory,
                                             'columns' (str : list of str) (metrics in list) - obligatory,
                                             'level1' (str : str) (company specific) - optional,
                                             'level2' (str : str) (company specific) - optional,
                                             'level3' (str : str) (company specific) - optional,
                                             'level4' (str : str) (company specific) - optional
    database_name (str): name of db. Values (names of db and id provided by MOAT) need to be defined in config file

Returns:
    pandas DataFrame
```
Additional keys for dictionary may be available for specific databases (e.g. slices).
It is company dependent.

#### Returns

* pandas.DataFrame


## Example usage

```python

from sroka.api.moat.moat_api import get_data_from_moat
input_data = {
    'start' : '20170808',
    'end' : '20170808',
    'level3' : '123456678',
    'columns' : ['date','impressions_analyzed','measurable_impressions']
}


data = get_data_from_moat(input_data, 'moat')

```
