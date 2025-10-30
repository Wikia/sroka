from sroka.api.google_drive.google_drive_api import (
    google_drive_sheets_read,
    google_drive_sheets_write,
    google_drive_sheets_upload,
    google_drive_sheets_create,
    google_drive_sheets_tab_names,
    google_drive_get_file_parents,
    google_drive_check_file_permissions,
    google_drive_change_file_permission,
    google_drive_transfer_ownership,
    google_drive_sheets_add_tab,
    google_drive_sheets_delete_tab,
    google_drive_move_file)
from sroka.api.google_ad_manager.gam_api import get_data_from_admanager, get_service_data_from_admanager

import pandas as pd
import unittest

class Test_Drive_Sheets_GAM(unittest.TestCase):

    TEST_FILE_ID = "..."     #[only for Subtests 8a/8b] Replace with the actual ID of TEST_FILE
    TEST_FOLDER_ID = "..."   #[only for Subtests 8a/8b] Replace with the actual ID of TEST_PARENT_FOLDER
    TEST_NEW_FOLDER = "..."  #Replacewith the actual ID of TEST_MOVE_FOLDER - the folder where you want to transfer your file
    YOUR_EMAIL = "..." #Replace with your actual e-mail
    OTHER_EMAIL_A = "..." #Replace with e-mail which will receive new permission to a file
    OTHER_EMAIL_B = "..." #Replace with e-mail where you want to transfer ownership - YOU WILL LOSE OWNER ACCESS TO THE FILE!

    def test_google_sheets(self):
        #Function for tasting Google Sheets & Google Drive functionality

        #Subtest 1: File creation
        with self.subTest(case='sheets_create'):
            new_sheet = google_drive_sheets_create('new_sheet')
            self.assertIsInstance(new_sheet, str)

        #Subtest 2: File writing
        with self.subTest(case='sheets_write'):
            temp_df_in = pd.DataFrame([['a']])
            #pylint: disable=E1128
            output = google_drive_sheets_write(temp_df_in, new_sheet, with_columns=False)
            self.assertIsNone(output)

        #Subtest 3: File reading
        with self.subTest(case='sheets_read'):
            temp_df_out = google_drive_sheets_read(new_sheet, 'Sheet1!A1:A2')
            self.assertTrue((temp_df_out == temp_df_in).all().all())

        #Subtset 4: Tab name reading
        with self.subTest(case='tab_name_read'):
            read_tab_name_out = google_drive_sheets_tab_names(new_sheet)
            self.assertIn('Sheet1', read_tab_name_out)

        #Subtset 5: Adding a tab
        with self.subTest('sheets_add_tab'):
            google_drive_sheets_add_tab(new_sheet, 'NEW TAB')
            add_tab_name_out = google_drive_sheets_tab_names(new_sheet)
            tab_length_in = len(add_tab_name_out)

            self.assertIn('NEW TAB', add_tab_name_out)

        #Subset 6: Removing a tab
        with self.subTest('sheets_delete_tab'):
            google_drive_sheets_delete_tab(new_sheet, 'NEW TAB')
            add_tab_name_out = google_drive_sheets_tab_names(new_sheet)
            tab_length_out = len(add_tab_name_out)

            self.assertNotIn('NEW TAB', add_tab_name_out)
            self.assertEqual(tab_length_out, tab_length_in-1)

        #Subtset 7: Data upload
        with self.subTest(case='sheets_upload'):
            temp_data_in = google_drive_sheets_upload(pd.DataFrame(
                [
                    ['2025-09-01', '60989056'],
                    ['2024-05-08', '58731240']
                ], 
                columns=['DATE', 'Total Impressions']), name='test_sheet')
            temp_id = google_drive_sheets_upload(temp_data_in, name='test')
            temp_data_out = google_drive_sheets_read(temp_id, 'Sheet1', first_row_columns=True)
            self.assertTrue((temp_data_out == temp_data_in).all().all())

        #Subtest 8a: Get file parents testing - SUCCESS testing
        with self.subTest(case='drive_parents_success'):
            parent_id_6a = google_drive_get_file_parents(self.TEST_FILE_ID)

            #1. Check the result type
            self.assertIsInstance(parent_id_6a, list)
            
            #2. Check the number of parents
            self.assertEqual(len(parent_id_6a), 1)
    
            #3. Check the specific parent ID
            self.assertEqual(parent_id_6a[0], self.TEST_FOLDER_ID)

        #Subtest 8b: Get file parents testing - SUCCESS testing
        with self.subTest(case='drive_parents_failure'):
            
            incorrect_id = 'THIS_IS_A_WRONG_ID'
            parent_id_6b = google_drive_get_file_parents(incorrect_id)

            self.assertEqual(parent_id_6b, [])

        #Subtest 9: Moving a file
        with self.subTest(case='drive_move_file'):
            temp_id_value = google_drive_sheets_create('TEST FILE PLEASE DELETE LATER')
            old_folder_list = google_drive_get_file_parents(temp_id_value)
            old_folder_id = old_folder_list[0]

            google_drive_move_file(temp_id_value, old_folder_id, self.TEST_NEW_FOLDER)

            folder_id_out = google_drive_get_file_parents(temp_id_value)
            self.assertEqual(folder_id_out, [self.TEST_NEW_FOLDER])

        #Subtest 10: Checking file permissions
        with self.subTest(case='check_file_permissions'):
            file_permissions_out = google_drive_check_file_permissions(new_sheet)
            
            #1. E-mail adress check
            self.assertIn(self.YOUR_EMAIL, file_permissions_out)

            #2. Role check
            expected_role = 'owner'
            actual_role = file_permissions_out[self.YOUR_EMAIL]

            self.assertEqual(expected_role, actual_role, f"Expected role '{expected_role}', but found '{actual_role}'")

        #Subtest 11: Changing permissions in a file
        with self.subTest(case='change_file_permission'):

            #1. Checking if new email is not marked as a user already
            self.assertNotIn(self.OTHER_EMAIL_A, file_permissions_out, "User is already listed in this document - change the OTHER_EMAIL_A global constant")

            #2. Checking if function returned True
            success_check = google_drive_change_file_permission(new_sheet, self.OTHER_EMAIL_A,'writer')
            self.assertTrue(success_check, "File permissions change failed")

            #3. E-mail adress check
            file_permissions_out = google_drive_check_file_permissions(new_sheet)
            self.assertIn(self.OTHER_EMAIL_A, file_permissions_out)

            #4. Role check
            user_a_expected_role = 'writer'
            user_a_actual_role = file_permissions_out[self.OTHER_EMAIL_A]
            self.assertEqual(user_a_expected_role, user_a_actual_role, f"Expected role '{user_a_expected_role}', but found '{user_a_actual_role}'")

        #Subtest 12: Changing ownershiop of a file
        with self.subTest(case='transfer_ownership'):

            #1. Checking if new email is not marked as a user already
            self.assertNotIn(self.OTHER_EMAIL_B, file_permissions_out, "User is already listed in this document - change the OTHER_EMAIL_B global constant")

            #2. Checking if function returned True
            success_check = google_drive_transfer_ownership(new_sheet, self.OTHER_EMAIL_B)
            self.assertTrue(success_check, "File ownership transfer failed")

            #3. E-mail adress check
            file_permissions_out = google_drive_check_file_permissions(new_sheet)
            self.assertIn(self.OTHER_EMAIL_B, file_permissions_out)

            #4. Role check
            user_b_expected_role = 'owner'
            user_b_actual_role = file_permissions_out[self.OTHER_EMAIL_B]
            self.assertEqual(user_b_expected_role, user_b_actual_role, f"Expected role '{user_b_expected_role}', but found '{user_b_actual_role}'")
    
    
    def test_google_admanager(self):
        #Function for tasting Google Ad Manager functionality

        #Subtest 1: Pulling data from Ad Manager
        with self.subTest(case="pulling_gam_data"):
            df_gam_in = pd.DataFrame([
            ['2024-03-01', 140913485],
            ['2024-03-02', 162338202]], columns=['Dimension.DATE',
                    'Column.TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS'])
            
            query = ""
            dimensions = ['DATE']
            columns = ['TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS']
            start_date = {'year': '2024',
                'month': '03',
                'day': '01'}
            end_date = {'year': '2024',
                'month': '03',
                'day': '02'}

            df_gam_out = get_data_from_admanager(query, dimensions, columns,
                                    start_date, end_date)
            self.assertTrue((df_gam_out == df_gam_in).all().all())

        #Subtest 2: Getting data for a certain service
        with self.subTest(case="get_service_data"):
            service_2 = "AdUnit"
            query_filter_2 = "WHERE status = 'ACTIVE' AND id IN ('22067796444', '22933978589')"
            columns_to_keep_2 = [
                "id",
                "parentId",
                "hasChildren",
                "parentPath",
                "adUnitCode",
                "targetWindow",
                "status"]
            expected_data_2 = [
                [
                    "22067796444",
                    "22067796441",
                    False,
                    '[{"id": "81570852", "name": "5441", "adUnitCode": "5441"}, {"id": "21787210647", "name": "vm1b.mr", "adUnitCode": "vm1b.mr"}, {"id": "21786989669", "name": "top_boxad", "adUnitCode": "top_boxad"}, {"id": "21788841311", "name": "smartphone", "adUnitCode": "smartphone"}, {"id": "22067796441", "name": "ns-curated", "adUnitCode": "ns-curated"}]',
                    "_fandom-all",
                    "BLANK",
                    "ACTIVE"],
                [
                    "22933978589",
                    "22933978586",
                    True,
                    '[{"id": "81570852", "name": "5441", "adUnitCode": "5441"}, {"id": "22933379790", "name": "iu", "adUnitCode": "iu"}, {"id": "22933978586", "name": "interstitial", "adUnitCode": "interstitial"}]',
                    "maw-metacritic",
                    "BLANK",
                    "ACTIVE"]]
            data_df_in = pd.DataFrame(expected_data_2,columns_to_keep_2)
            data_df_out = get_service_data_from_admanager(service_2, query_filter_2,columns_to_keep)
            self.assertTrue((data_df_out == data_df_in).all().all())
            
            


#TBD Ad Manager tests
    # Test for get_service_data_from_admanager function CustomTargetingKeys
    service = "CustomTargetingKeys"
    query_filter = "WHERE id in ('415092','415212')"

    columns_to_keep = [
        "id",
        "name",
        "displayName",
    ]


    expected_data = [
        [
            415092,
            "pform",
            "Console",
        ],
        [
            415212,
            "gnre",
            "Genre",
        ],
    ]

    expected_df = pd.DataFrame(
        expected_data,
        columns=columns_to_keep
    )

    actual_df = get_service_data_from_admanager(
        service=service,
        query_filter=query_filter,
        columns_to_keep=columns_to_keep,
    )

    pd.testing.assert_frame_equal(actual_df, expected_df)

    # Test for get_service_data_from_admanager function CustomTargetingValues
    service = "CustomTargetingValues"
    query_filter="WHERE customTargetingKeyId = '415692' and id in ('45255435372','45255435732')"

    columns_to_keep = [
        "id",
        "name",
        "status",
    ]

    expected_data = [
        [
            45255435372,
            "top_right_boxad",
            "ACTIVE",
        ],
        [
            45255435732,
            "home_top_right_boxad",
            "ACTIVE",
        ],
    ]

    expected_df = pd.DataFrame(
        expected_data,
        columns=columns_to_keep
    )

    actual_df = get_service_data_from_admanager(
        service=service,
        query_filter=query_filter,
        columns_to_keep=columns_to_keep,
    )
    pd.testing.assert_frame_equal(actual_df, expected_df)"""
