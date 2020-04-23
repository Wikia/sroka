# GA API

## Methods

### `ga_request(input_dict, print_sample_size, sampling_level)`

#### Arguments


* dict `input_dict` - obligatory
* boolean `print_sample_size` - optional, will print the % of data used to calculate results. Default value is False.
* string `sampling_level` - optional, specifies precision of your query. Available values are: 'DEFAULT', 'FASTER', 
or 'HIGHER_PRECISION'. By default query is called with 'HIGHER_PRECISION'. 



#### Returns

* pandas.DataFrame

## Example usage

```python
from sroka.api.ga.ga import ga_request

request = {
"ids" : "ga:12345678",
"start_date": "2018-05-01",
"end_date": "2018-05-07",
"metrics": "ga:pageviews,ga:sessions,ga:bounceRate,ga:avgTimeOnPage,ga:avgPageLoadTime",
"filters": "ga:deviceCategory=~desktop,ga:deviceCategory=~tablet",
"segment": "",
"sort": "-ga:pageviews",
"dimensions" : "ga:day"
}

df_ga = ga_request(request, print_sample_size=True, sampling_level='FASTER')

```


### `ga_request_all_data(input_dict, start_index, page_size, max_pages, print_sample_size, sampling_level)`

Retrieves all data matched by the given query parameters. It internally uses request pagination to fetch all available rows.

Even though the purpose of this function is to download all rows from GA at once, you can limit the range of values:

* to specify the start_index use the function parameters instead of passing it as one of the input_dict values
* instead of max_results in input_dict, you can specify the maximal number of pages to be retrieved (and their size)

Note that this function overwrites the `start_index` and `max_results` values of the `input_dict` dictionary!

#### Arguments

* `input_dict` (obligatory) - the dictionary of GA request parameters
* `start_index` - the index of the first element to be retrieved (integer, optional, default = 1, note that **indexes start from 1!**)
* `page_size` - the number of elements retrieved in a single request (integer, optional, default = 10000, min value = 1)
* `max_pages` - the max number of pages to retrieve, None if all available pages (integer, optional, default = None, min value = 1)
* `print_sample_size` - if True, prints the sample size of every request (boolean, optional, default = False)
* `sampling_level` - the GA sampling level (optional, default = HIGHER_PRECISION, valid values: 'DEFAULT', 'FASTER', 'HIGHER_PRECISION')

#### Returns

* pandas.DataFrame

#### Example usage

```python
from sroka.api.ga.ga import ga_request_all_data

request = {
  'ids': 'ga:12345678',
  'start_date': '2020-03-15',
  'end_date': '2020-03-15',
  'metrics': 'ga:pageviews,ga:sessionsPerUser,ga:avgSessionDuration,ga:pageviewsPerSession,ga:bounceRate,ga:users',
  'filters': 'ga:deviceCategory=~desktop,ga:deviceCategory=~tablet',
  'dimensions': 'ga:deviceCategory,ga:channelGrouping',
  'sort': '-ga:pageviews'
}

df_ga = ga_request_all_data(request)
```