# Rubicon API

## Methods


### `get_data_from_rubicon(rubicon_dict)`


#### Arguments

* dict `rubicon_dict` - dictionary with keys:
    * 'start' (str : str) (Start date in ISO-8601 format, including time zone.) - obligatory,
    * 'end' (str : str) (End date in ISO-8601 format, including time zone.) - obligatory,
    * 'dimensions' (str : list of str) (dimensions that we want included as columns in list) - obligatory,
    * 'metrics' (str : list of str) (metrics that we want included as columns in list) - obligatory
    * 'filters' (str: list of str) (filters that we want to be included in list) - obligatory

* currency (str): currency to be used

Full documentation (additional features) of Rubicon API is available at:
        https://resources.rubiconproject.com/resource/publisher-resources/performance-analytics-api/
        (you need to be logged in with provided credentials)

#### Returns

* pandas DataFrame

## Usage

```python
from sroka.api.rubicon.rubicon_api import get_data_from_rubicon


input_data_prebid = {
    'start' : '2017-08-25T00:00:00-07:00',
    'end' : '2017-08-31T23:59:59-07:00',
    'dimensions' : ['date'],
    'metrics' : ['revenue',
                 'paid_impression'
                ],
    'filters' : ['dimension:country_id==PL']
}

data_rubicon = get_data_from_rubicon(input_data_prebid)
data_rubicon
```
