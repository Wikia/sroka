import os, re
from googleapiclient.discovery import build
import sroka.config.config as config


def is_valid_email(email_string: str):
    """Checks if a string structurally matches the email regex.
    Args:
        email_string (str): The e-mail address.
    """

    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.fullmatch(email_regex, email_string):
        return True
    return False

def service_builder(service_type: int, version: str):
    """
    Creates a service with proper credentials for a selected build from the service_type variable: 'sheets' or 'drive'

    Args:
        service_type (int): Acceptable services:
                    - PRESS 1 for 'sheets': Google Sheets operations
                    - PRESS 2 for 'drive': Google Drive operations
        version (str): Acceptable versions:
                    - 'v2'
                    - 'v3'
                    - 'v4'

    Returns:
        TBTested
    """

    try:
        valid_service_type = [1, 2]
        if service_type not in valid_service_type:
            raise ValueError('Available services: [1] sheets, [2] drive')   
    except ValueError as e:
        print(f"An incorrect role has been used in the fucntion - {e}")
        return False
    
    try:
        valid_version = ['v2', 'v3', 'v4']
        if version.lower() not in valid_version:
            raise ValueError('Available versions: v2, v3, v4')   
    except ValueError as er:
        print(f"An incorrect version has been used in the fucntion - {er}")
        return False

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_read.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    if service_type == 1:
        service = build('sheets', version, credentials=credentials)
        return service
    if service_type == 2:
        service = build('drive', version, credentials=credentials)
        return service
    else:
        return False