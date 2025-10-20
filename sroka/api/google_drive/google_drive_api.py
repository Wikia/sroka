from __future__ import print_function

import os

import numpy as np
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import sroka.config.config as config


def google_drive_sheets_read(sheetname_id: str, sheet_range: str, first_row_columns=False):
    """
    Reads data from a specified range in a Google Spreadsheet.

    Args:
        sheetname_id (str): The ID of the Spreadsheet.
        sheet_range (str): The A1 notation range of the data to retrieve (e.g., 'Sheet1!A21:D37').
        first_row_columns (bool): If True, treats the first row of the retrieved data
                                            as the column headers for the resulting DataFrame and removes
                                            that row from the data. 
                                            Defaults to False.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the data read from the sheet.
                      Returns an empty DataFrame if an HttpError occurs during the API call.
    """

    # Authenticate and construct service.
    scope = 'https://www.googleapis.com/auth/spreadsheets.readonly'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_sheets/token_read.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('sheets', 'v4', credentials=credentials)

    sheet = service.spreadsheets()
    try:
        result = sheet.values().get(spreadsheetId=sheetname_id,
                                    range=sheet_range).execute()
    except HttpError as err:
        print("HTTP error occurred. Error:")
        print(err)
        return pd.DataFrame([])

    values = result.get('values', [])
    df = pd.DataFrame(values)

    if first_row_columns:
        df.columns = df.loc[0, :].values
        df.drop(0, inplace=True)
        df.reset_index(drop=True, inplace=True)
    return df


def google_drive_sheets_create(name: str):
    """
    Creates a new, empty Google Spreadsheet file with the specified name.

    Args:
        name (str): The name (title) for the new Spreadsheet file.

    Returns:
        str: The ID of the newly created Spreadsheet. Returns an empty
             string ('') if an HttpError occurred during creation.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_create.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('sheets', 'v4', credentials=credentials)
    spreadsheet = {
        'properties': {
            'title': name
        }
    }
    try:
        spreadsheet = service.spreadsheets().create(body=spreadsheet,
                                                    fields='spreadsheetId').execute()
    except HttpError as err:
        print("HTTP error occurred. Error:")
        print(err)
        return ''

    return spreadsheet.get('spreadsheetId')


def google_drive_sheets_add_tab(spreadsheet_id: str, name: str):
    """
    Function to add new tab to the existing spreadsheet with a specified name.

    Args:
        spreadsheet_id (str): The ID of the Spreadsheet to modify.
        name (str): The title/name to assign to the new sheet tab.

    Returns:
        str: The ID of the spreadsheet.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_create.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('sheets', 'v4', credentials=credentials)

    data = [{
        'addSheet': {
            'properties': {'title': name}
        }
    }]
    try:
        spreadsheet = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                         body={'requests': data},
                                                         fields='replies/addSheet').execute()
    except HttpError as err:
        print("HTTP error occurred. Error:")
        print(err)
        return spreadsheet_id

    return spreadsheet.get('spreadsheetId')


def google_drive_sheets_write(data, spreadsheet_id: str, sheet_range='Sheet1!A1',
                              with_columns=True, with_index=False):
    """
    Function to write to existing google sheet starting at a given range.

    Args:
        data: Ideall, a pandas DataFrame.
        spreadsheet_id (str): The ID of the Spreadsheet.
        sheet_range (str): The range to start writing data - If not changed, the deafult value is 'Sheet1!A1'.
        with_columns (bool): If True, includes the DataFrame's column headers as the first row. 
                            The deafult value is True.
        with_index (bool, optional): If True, includes the DataFrame's index as the first column. 
                            The deafult value is False.

    Returns:
        None: The function primarily prints success/error messages and returns None upon completion or error.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_create.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('sheets', 'v4', credentials=credentials)

    values = data.values.tolist()
    columns = list(data.columns)

    if with_index:
        if values:
            values = np.append(np.array([list(data.index)]).T, values, axis=1).tolist()
        columns = ['index'] + list(data.columns)
    if with_columns:
        if values:
            values = np.append([columns], values, axis=0).tolist()
        else:
            values = [columns]

    body = {
        'values': values
    }
    try:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=sheet_range,
            body=body, valueInputOption='RAW').execute()
    except HttpError as err:
        print("HTTP error occurred. Error:")
        print(err)
        return None

    print('Successfully uploaded to google sheets: https://docs.google.com/spreadsheets/d/' + spreadsheet_id)
    return None


def google_drive_sheets_upload(data, name: str,
                               with_columns=True, with_index=False):
    """
    Creates a new Google Spreadsheet and uploads data to its first sheet.

    Args:
        data: Ideall, a pandas DataFrame.
        name (str): The name to assign to the new Google Spreadsheet file.
        with_columns (bool): If True, includes the DataFrame's column headers as the first row. 
                            The deafult value is True.
        with_index (bool, optional): If True, includes the DataFrame's index as the first column. 
                            The deafult value is False.

    Returns:
        str: The ID of the newly created Google Spreadsheet.
    """

    spreadsheet_id = google_drive_sheets_create(name)
    google_drive_sheets_write(data, spreadsheet_id, sheet_range='Sheet1!A1',
                              with_columns=with_columns, with_index=with_index)
    return spreadsheet_id


def google_drive_move_file(file_id: str, old_folder_id: str, new_folder_id: str):
    """
    Moves a file from one folder to another in Google Drive.

    The move operation is done by updating the file's 'parents' property:
    1. Remove the ID of the old folder.
    2. Add the ID of the new folder.

    Args:
        service: The initialized Google Drive API service client.
        file_id (str): The ID of the file to move.
        old_folder_id (str): The ID of the current parent folder. Use 'root' 
                             if the file is in the main Drive page.
        new_folder_id (str): The ID of the target parent folder. Use 'root' 
                             to move the file to the main Drive page.

    Returns:
        bool: True if the file was moved successfully, False otherwise.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_read.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)


    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('drive', 'v3', credentials=credentials)

    try:
        #Prepare the update request body (empty, as we only manipulate parents)
        #pylint: disable=E1101
        file = service.files().update(
            fileId=file_id,
            addParents=new_folder_id,
            removeParents=old_folder_id,
            #We only need the ID and parents list back to confirm the change
            fields='id, parents'
        ).execute()

        #Check if the new folder ID is in the updated parents list
        if new_folder_id in file.get('parents', []):
            print(f"Success: File '{file_id}' moved from '{old_folder_id}' to '{new_folder_id}'.")
            return True
        else:
            print(f"Warning: File '{file_id}' updated, but new parent not confirmed.")
            return False

    except HttpError as error:
        print(f"An API error occurred while moving file: {error}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while moving file: {e}")
        return False


def google_drive_sheets_delete_tab(spreadsheet_id: str, tab_name: str):
    """
    Deletes a tab from a Google Sheets spreadsheet by name.

    Args:
        spreadsheet_id (str): The ID of the spreadsheet.
        tab_name (str): The name of the tab to delete.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_create.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(
        authorized_user_file,
        key_file_location,
        scope
    )

    service = build('sheets', 'v4', credentials=credentials)

    try:
        # Retrieve spreadsheet info to find sheet/tab ID
        #pylint: disable=E1101
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        sheet_id = None

        for sheet in sheets:
            if sheet['properties']['title'] == tab_name:
                sheet_id = sheet['properties']['sheetId']
                break

        if sheet_id is None:
            print(f"Sheet '{tab_name}' not found in spreadsheet {spreadsheet_id}.")
            return None

        # Delete the tab by ID
        request_body = {
            'requests': [{'deleteSheet': {'sheetId': sheet_id}}]
        }

        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request_body
        ).execute()
        return response.get('spreadsheetId')

    except HttpError as err:
        print("HTTP error occurred during tab deletion:")
        print(err)
        return None
    

def google_drive_get_file_parents(file_id: str):
    """
    Retrieves the parent folder IDs for a specified file on Google Drive.
       
    Arg:
        file_id (str): The ID of the file whose parent folders are to be retrieved.
    
    Retruns:
        list: A list of string IDs for the parent folder(s) of the file. Returns an empty list on failure.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_read.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('drive', 'v3', credentials=credentials)

    try:
        #pylint: disable=E1101
        file_metadata = service.files().get(fileId=file_id, fields='parents').execute()
        
        #Returns a list of parent IDs (most files only have one parent)
        print(f"Success: File '{file_id}' has parent(s): {file_metadata.get('parents', [])}")
        return file_metadata.get('parents', []) 
    except HttpError as error:
        print(f"Error getting parents: {error}")
        return []


def google_drive_transfer_ownership(file_id: str, new_owner_email: str):
    """
    Transfers ownership of a file to a new user.

    This function creates a new permission with 'owner' role for the target
    email and sets 'transferOwnership' to True, which automatically demotes
    the current owner (the user running this code) to an editor role.

    Args:
        service: The initialized Google Drive API service client.
        file_id (str): The ID of the file to transfer ownership of.
        new_owner_email (str): The email address of the new owner.

    Returns:
        bool: True if the ownership request was successful, False otherwise.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_read.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('drive', 'v3', credentials=credentials)

    try:
        #Define the permission body for the new owner
        permission_body = {
            'type': 'user',
            'role': 'owner',
            'emailAddress': new_owner_email
        }

        #Create the permission, triggering the ownership transfer
        #pylint: disable=E1101
        permission = service.permissions().create(
            fileId=file_id,
            body=permission_body,
            transferOwnership=True,
            #Limit on response data
            fields='id'
        ).execute()

        print(f"Success: Ownership transfer initiated for file '{file_id}' to '{new_owner_email}'.")
        print("Note: The previous owner will be demoted to an editor role.")
        return True

    except HttpError as error:
        print(f"An API error occurred during ownership transfer: {error}")
        print("Check if the new owner is in the same Google Workspace/domain and if settings allow external transfers.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during ownership transfer: {e}")
        return False


def google_drive_change_file_permission(file_id: str, user_email: str, role: str):


    """
    Adds a new permission to a file, granting a specific role (view/edit/comment) 
    to a user via email.

    Args:
        service: The initialized Google Drive API service client.
        file_id (str): The ID of the file to modify permissions for.
        user_email (str): The email address of the user receiving the permission.
        role (str): The access level to grant. Common values:
                    - 'reader': View-only access
                    - 'writer': Edit access
                    - 'commenter': Comment access

    Returns:
        bool: True if the permission was added successfully, False otherwise.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_read.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('drive', 'v3', credentials=credentials)

    try:
        #Define the permission body for the target user and role
        permission_body = {
            'type': 'user',
            'role': role,
            'emailAddress': user_email
        }
        
        #Insert the new permission
        #pylint: disable=E1101
        permission = service.permissions().create(
            fileId=file_id,
            body=permission_body,
            fields='id'
        ).execute()

        print(f"Success: Granted '{role}' permission on file '{file_id}' to '{user_email}'.")
        return True

    except HttpError as error:
        print(f"An API error occurred while changing permission: {error}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while changing permission: {e}")
        return False


def google_drive_sheets_tab_names(spreadsheet_id: str):
    """
    Retrieves the names of all tabs (worksheets) in a Google Sheets spreadsheet.

    Args:
        data: Ideall, a pandas DataFrame.
        spreadsheet_id (str): The ID of the Spreadsheet.

    Returns:
        str: The list of tab names.
    """
    
    scope = 'https://www.googleapis.com/auth/spreadsheets.readonly'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_read.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(
        authorized_user_file,
        key_file_location,
        scope
    )

    service = build('sheets', 'v4', credentials=credentials)
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_names = [s['properties'].get('title') for s in metadata.get('sheets', []) if 'properties' in s]

    return sheet_names


def google_drive_check_file_permissions(file_id: str):
    """
    Retrieves all user-specific permissions for a file and returns them as a map 
    of {email: role}.
    This function maps only permissions linked to a specific user (not 'anyone',
    'domain', or 'group')and that have an email address present.

    Args:
        file_id (str): The ID of the file to check.

    Returns:
        dict: A map where keys are email addresses (str) and values are 
              their roles (str, e.g., 'owner', 'writer', 'reader'). 
              Returns an empty dictionary on failure.
    """

    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/google_drive/token_read.json')
    os.makedirs(os.path.dirname(authorized_user_file), exist_ok=True)

    credentials = config.set_google_credentials(authorized_user_file,
                                                key_file_location,
                                                scope)

    service = build('drive', 'v3', credentials=credentials)
    
    # Creating a dictionary
    permission_dict = {}

    try:
        # Retrieve the list of all permissions for the file
        # pylint: disable=E1101
        permissions = service.permissions().list(
            fileId=file_id,
            fields='permissions(emailAddress, role, type)' 
        ).execute()

        # Iterate through the permissions and build the map
        for permission in permissions.get('permissions', []):
            p_type = permission.get('type')
            p_email = permission.get('emailAddress')
            p_role = permission.get('role')

            if p_type == 'user' and p_email:
                permission_dict[p_email] = p_role

        return permission_dict

    except HttpError as error:
        print(f"An API error occurred while checking permissions: {error}")
        # Return an empty dict on error for consistent return type
        return {}
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {}