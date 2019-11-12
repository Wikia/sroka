from __future__ import print_function

import os

import numpy as np
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import sroka.config.config as config


def google_drive_sheets_read(sheetname_id, sheet_range, first_row_columns=False):
    """
    Function to read from google sheets
    """
    # Authenticate and construct service.
    scope = 'https://www.googleapis.com/auth/spreadsheets.readonly'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/token_read.json')

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


def google_drive_sheets_create(name):
    """
    Function to create a google sheet file
    """
    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/token_create.json')

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


def google_drive_sheets_add_tab(spreadsheet_id, name):
    """
    Function to add new tab to existing spreadsheet
    """
    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/token_create.json')

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


def google_drive_sheets_write(data, spreadsheet_id, sheet_range='Sheet1!A1',
                              with_columns=True, with_index=False):
    """
    Function to write to existing google sheet
    """
    scope = 'https://www.googleapis.com/auth/drive'
    key_file_location = config.get_file_path('google_drive')
    authorized_user_file = os.path.expanduser('~/.cache/token_create.json')

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


def google_drive_sheets_upload(data, name,
                               with_columns=True, with_index=False):
    """
    Function to upload data to google sheets
    """
    spreadsheet_id = google_drive_sheets_create(name)
    google_drive_sheets_write(data, spreadsheet_id, sheet_range='Sheet1!A1',
                              with_columns=with_columns, with_index=with_index)
    return spreadsheet_id
