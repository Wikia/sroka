<p align="center"><img width="500" src="images/sroka.png"/></p>

# sroka package

Package providing simple Python access to data in:
* Google Analytics
* Google AdManager (GAM earlier DoubleClick for Publishers, DFP)
* Google sheets
* Google BigQuery
* MOAT
* Qubole
* Rubicon
* AWS Athena
* AWS s3
* MySQL
* neo4j

Sroka library was checked to work for Python **3.7, 3.8 and 3.9**.

## Developers

Install requirements and enable custom githooks:
```
pip install -r requirements.txt
git config --local core.hooksPath .githooks/
```  
Check style with flake8:
```
flake8 .
```

Please target Pull Requests against `dev` branch.

## Installation

### Pypi last release

```pip install sroka```

### GitHub version (beta version)

```pip install git+ssh://git@github.com/Wikia/sroka```


## Configuration

in home folder create `~/.sroka_config` (hidden folder) file where you will store:
* ```config.ini``` file based on ```config.sample.ini``` with information to access Qubole, MOAT, Athena, S3 and Rubicon
* ```client_secrets.json``` for GA access
* ```ad_manager.json``` for GAM access
* ```credentials.json``` for Google sheets access
* ```bigquery_credentials.json``` for BigQuery access

Alternatively, you may set localization of your files during analysis:

```python
from sroka.config.config import setup_env_variables
from sroka.config.config import setup_client_secret
from sroka.config.config import setup_admanager_config
from sroka.config.config import setup_bigquery_config
from sroka.config.config import setup_google_sheets_credentials
setup_env_variables('/file_path/config.ini')
setup_client_secret('/file_path/client_secrets.json')
setup_admanager_config('/file_path/ad_manager.json')
setup_bigquery_config('/file_path/bigquery_credentials.json')
setup_google_sheets_credentials('/file_path/credentials.json')
```

## Getting GA, GAM, BigQuery and Google docs jsons with secrets

### Google Analytics

1.	Use [this wizard](https://console.developers.google.com/flows/enableapi?apiid=analytics.googleapis.com) 
to create or select a project in the Google Developers Console and automatically turn on the API. Click Continue, then Go to credentials.
2.	On the Add credentials to your project page, click the Cancel button.
3.	At the top of the page, select the OAuth consent screen tab. Select an Email address, enter a Product name if not already set, and click the Save button.
4.	Select the Credentials tab, click the Create credentials button and select OAuth client ID.
5.	Select the application type Other, enter the chosen name, and click the Create button.
6.	Click OK to dismiss the resulting dialog.
7.	Click the file_download (Download JSON) button to the right of the client ID.


### GAM

1. Follow [these instructions](https://developers.google.com/ad-manager/api/authentication#service) 
    - while adding a service account note that the role needs to have necessary viewing and reporting permissions.

You should end up with .json (!) file with credentials

2. Make sure the *Name* in "OAuth 2.0 client IDs" matches the *service account* in "Service account keys": [here](https://console.developers.google.com/apis/credentials)
4. Create GAM account as service account not a new user: https://support.google.com/admanager/answer/6078734?hl=en
3. Once you have a service account, it can be used to access data in different networks. Simply add it as a new service account through GAM UI of the second network.
4. Additional information can be specified in ```config.ini``` file:
* network code - a default value that can be overwritten in a function call
* application name - custom name of your network, if not specified, a generic value will be passed.


### Google drive sheets credentials

In order to authorize in Google Sheets you need to generate credentials in Google Console:
* [Create project and enable Sheets API](https://developers.google.com/workspace/guides/create-project)
* [Configure OAuth consent screen](https://developers.google.com/workspace/guides/create-credentials#configure_the_oauth_consent_screen)
* [Add desktop credentials](https://developers.google.com/workspace/guides/create-credentials#desktop)

You should end up with `credentials.json` file that should be downloaded to `~/.sroka_config` folder.

### Google BigQuery credentials
Go to [link](https://cloud.google.com/bigquery/docs/reference/libraries) and follow up instructions
within ```Setting up authentication``` section. You should end up with ```bigquery_credentials.json```
json file that should be downloaded to `~/.sroka_config` folder. 


## Getting credentials & access tokens

### Qubole

1. Find your Qubole API Token (go to user -> My Profile -> my_account -> API Token -> show)
2. Copy your Qubole API Token to ```config.ini``` file


### Athena and s3 credentials

1. You should have your aws_access_key_id and aws_secret_access_key from registration process in AWS console.
2. s3bucket_name can be found in AWS console in Athena view when you click `Settings`, there you have `Query result location`.
The name of location without `s3://` and `/` is what you need.
3. For Athena usage you need to set also region (AWS regional endpoint), e.g. `'us-east-1'`


### Rubicon credentials
1. You should have your id, username and password from Rubicon
2. Copy values to ```config.ini``` file in relevant fields


### MySQL connection information

1. In order to connect to a remote MySQL server, you need to provide the `host` and `port` values in the configuration. If it is accessible through a unix socket, you need to provide the path to this socket instead in the `unix_socket` configuration field.
2. If the MySQL server is protected by user credentials, you need to provide the `user` and `password` values in the configuration.
3. You can optionally specify the database to which you want to connect in the `database` configuration field.

## Common issues

### macOS

If you see an error like `ValueError: unknown locale: UTF-8`

Please add to `~/.bash_profile` lines like this:

```
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
```
### installing sroka

1. If `PyYAML` package is not building correctly, it may be caused by the fact that newer versions of pip won’t uninstall the package because it’s handled by disutils. Please install `PyYAML` package first with `--ignore-installed` flag. 

2. If numpy gets messed up during sroka installation it is probably caused by multiple versions installed. Please uninstall all using pip uninstall and then reinstall latest one.

### Google APIs cached files

If you encounter RefreshError similar to 
`google.auth.exceptions.RefreshError: ('invalid_grant: Bad Request', '{\n  "error": "invalid_grant",\n  "error_description": "Bad Request"\n}')`
, try removing all files from `~/.cache` directory.

## Credits

All people that contributed to sroka development before going opensource (including CR and QA):
* [martynaut](https://github.com/martynaut)
* [dorotamierzwa](https://github.com/dorotamierzwa)
* [fraszczakszymon](https://github.com/fraszczakszymon)
* [bckatarzyna](https://github.com/bckatarzyna)
* [jacekbj](https://github.com/jacekbj)
* [nandy-andy](https://github.com/nandy-andy)
* [dmnsobczak](https://github.com/dmnsobczak)
* [szczeles](https://github.com/szczeles)
* [kvas-damian](https://github.com/kvas-damian)
* [pnather](https://github.com/pnather)
* [philthyharry](https://github.com/philthyharry)
