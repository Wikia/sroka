import sroka.config.config as config
from sroka.api.helpers.helpers_input import input_check

def query_snowflake(query, filename=None):

    if not input_check(query, [str]):
        return return_on_exception(filename)

    if not input_check(filename, [str, type(None)]):
        return return_on_exception(filename)

    if filename == '':
        print('Filename cannot be empty')
        return return_on_exception(filename)

    try:
        s3_bucket = config.get_value('aws', 's3bucket_name')
        key_id = config.get_value('aws', 'aws_access_key_id')
        access_key = config.get_value('aws', 'aws_secret_access_key')
        region = config.get_value('aws', 'aws_region')
    except (KeyError, NoOptionError) as e:
        print('No credentials were provided. Error message:')
        print(e)
        return return_on_exception(filename)