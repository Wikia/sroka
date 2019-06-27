# Google drive API

## Methods


### `google_drive_sheets_read(sheetname_id, sheet_range)`
Read from existing google sheet


#### Arguments

* string `sheetname_id` - id of google sheet
* string `sheet_range` - range of data in format `sheet_name!range` (range within sheet is optional) e.g. `Sheet1!A1:E5` or `Sheet1`
* boolean `first_row_columns` - whether to use first row as columns or not. Defaults to `False`.

#### Returns

* DataFrame

#### Usage

```python
from sroka.api.google_drive.google_drive_api import google_drive_sheets_read

df = google_drive_sheets_read('1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms', 'Class Data!A1:E5')
```

### `google_drive_sheets_create(name)`
Create new empty google sheet


#### Arguments

* string `name` - title for created google sheet

#### Returns

* string - id of google sheet

#### Usage

```python
from sroka.api.google_drive.google_drive_api import google_drive_sheets_create

google_drive_sheets_create('new sheet')
```


### `google_drive_sheets_write(data, sheetname_id, sheet_range, with_columns, with_index)`
Write to existing google sheet

#### Arguments

* pandas DataFrame `data` - data we want to write into sheets
* string `sheetname_id` - id of google sheet
* string `sheet_range` - range of data in format `sheet_name!range` (range within sheet is optional) e.g. `Sheet1!A1:E5` or `Sheet1`
* bool `with_columns` - if column names should be included in sheets
* bool `with_index` - if index should be included in sheets

#### Returns

nothing, prints link to updated google sheets

#### Usage

```python
from sroka.api.google_drive.google_drive_api import google_drive_sheets_write


google_drive_sheets_write(df, '1MpQyOyjgWSqvf48QG0PoxVEaG-xIxpiKRh9fFrzvOUQ', 'Sheet1',
                         with_columns=True, with_index=False)
```

### `google_drive_sheets_upload(data, name, with_columns, with_index)`
Create and write to new google sheet


#### Arguments

* pandas DataFrame `data` - data we want to write into sheets
* string `name` - title for created google sheet
* bool `with_columns` - if column names should be included in sheets
* bool `with_index` - if index should be included in sheets

#### Returns

* string - id of google sheet

#### Usage

```python
from sroka.api.google_drive.google_drive_api import google_drive_sheets_upload


google_drive_sheets_upload(df, 'New data sheet name',
                           with_columns=True, with_index=True)
```

### `google_drive_sheets_add_tab(spreadsheet_id, name)`
Add new sheet (tab) to existing spreadsheet.


#### Arguments

* string `spreadsheet_id` - Id of spreadsheet
* string `name` - title for newly created tab

#### Returns

* string - id of google spreadsheet

#### Usage

```python
from sroka.api.google_drive.google_drive_api import google_drive_sheets_add_tab


google_drive_sheets_add_tab('1HwDCaegQ-dboSvCE4NByPelOXUyEEGTVN7MeoL-vRnE', 'New Tab Name')
```

