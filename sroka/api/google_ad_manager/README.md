# GAM API

## Methods

### `get_data_from_admanager(query, dimensions, columns, start_date, stop_date, custom_field_id, dimension_attributes, network_code)`

#### Arguments


* string `query` - obligatory
* list `dimensions` - obligatory
* list `columns` - obligatory
* dict `start_date` - obligatory
* dict `stop_date` - obligatory
* list `custom_field_id` -  list of ints, default=[], not obligatory  IMPORTANT: to use custom field id corresponding dimension is needed
* list `dimension_attributes` -  list of strings, default=[], not obligatory  IMPORTANT: to use dimension attribute corresponding dimension is needed
* int `network_code` - default value taken from config.ini file. If the same service account has access to more than one network, the default value can be overwritten with this argument.

What is `custom_field_id` ?
* Custom fields are additional fields that you can apply to orders, line items, and creatives. These fields can be used to organize objects in reports. You apply the field to a particular object by setting a value for it.
* To find id of custom field go to GAM dashboard -> Admin -> Global settings -> Custom Fields -> Choose Custom Field and id can be found in URL

What are `dimension_attributes` ?
* Dimension attributes provide additional fields associated with a dimension like start/end date, goal, pacing. These fields can be used to organize objects in reports and get non-metrical information regarding specific dimension.
* List of the dimension atrributes can be found on [Google Ad Manager Help](https://support.google.com/admanager/answer/2875225?hl=en&ref_topic=7492017) or [GAM API documentation](https://developers.google.com/ad-manager/api/reference/v202011/ReportService.DimensionAttribute)


#### Returns

* pandas.DataFrame


## Example usage

```python
from sroka.api.google_ad_manager.gam_api import get_data_from_admanager

start_day = '01'
end_day='01'
start_month = '08'
end_month = '08'
year = '2017'

# Data from GAM - report
query = "WHERE CUSTOM_TARGETING_VALUE_ID=12345"
dimensions = ['DATE', 'LINE_ITEM_NAME']
dimension_attributes = ['LINE_ITEM_GOAL_QUANTITY']
custom_field_id = [54321]
columns = ['TOTAL_INVENTORY_LEVEL_IMPRESSIONS', 
           'TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS',
           'TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS']
start_date = {'year': year,
              'month': start_month,
              'day': start_day}
stop_date = {'year': year,
             'month': end_month,
             'day': end_day}

data = get_data_from_admanager(query, dimensions, columns, start_date, stop_date, custom_field_id=custom_field_id, dimension_attributes=dimension_attributes, network_code=1234)

```

### `get_users_from_admanager(query, dimensions, network_code)`

#### Arguments


* string `query` - obligatory (does not require WHERE clause)
* list `dimensions` - obligatory IMPORTANT: roleName is a readonly attribute
* int `network_code` - default value taken from config.ini file. If the same service account has access to more than one network, the default value can be overwritten with this argument.

#### Returns

* pandas.DataFrame

## Example usage

```python
from sroka.api.google_ad_manager.gam_api import get_users_from_admanager

# Data from GAM - user list
query = "WHERE roleName IN ('Administrator')"
dimensions = ['id', 'name']

data = get_users_from_admanager(query, dimensions, network_code=1234)

```

### `get_companies_from_admanager(query, dimensions, network_code)`

#### Arguments


* string `query` - obligatory (does not require WHERE clause)
* list `dimensions` - obligatory
* int `network_code` - default value taken from config.ini file. If the same service account has access to more than one network, the default value can be overwritten with this argument.

#### Returns

* pandas.DataFrame

## Example usage

```python
from sroka.api.google_ad_manager.gam_api import get_companies_from_admanager

# Data from GAM - company list
query = "WHERE type IN ('Advertiser')"
dimensions = ['id', 'name']

data = get_companies_from_admanager(query, dimensions, network_code=1234)

```

### `get_inventory_from_admanager(inventory_type, filter_text, network_code)`

#### Arguments
* inventory_type: The type of inventory to fetch. Must be a key in the 
                inventory_service_map (e.g., 'AdUnit').
* query_filter: An optional PQL-like 'WHERE' clause to filter the results.
             For example: "WHERE status = 'ACTIVE'". Do not include
             'ORDER BY' or 'LIMIT' clauses.
* columns_to_keep: An optional list of column names to keep in the output DataFrame.
            If None, provides all the columns.
* network_code: The GAM network code to use.

#### Returns

* pandas.DataFrame

## Example usage

```python
from sroka.api.google_ad_manager.gam_api import get_inventory_from_admanager

# Data from GAM - company list
inventory_type = "AdUnit"
filter_text = "WHERE status = 'ACTIVE'"

data = get_inventory_from_admanager(inventory_type, filter_text, network_code=1234)

```