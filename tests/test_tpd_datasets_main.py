import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

import unittest
from unittest.mock import MagicMock, patch
import json
from src.data_ingestion.tpd_datasets import process_tpd_sectionals_data

class TestProcessTPDSectionalsData(unittest.TestCase):

    @patch('src.data_ingestion.tpd_datasets.os.listdir')
    @patch('src.data_ingestion.tpd_datasets.os.path.isdir')
    @patch('src.data_ingestion.tpd_datasets.open', new_callable=MagicMock)
    @patch('src.data_ingestion.tpd_datasets.extract_course_code')
    @patch('src.data_ingestion.tpd_datasets.extract_race_date')
    @patch('src.data_ingestion.tpd_datasets.gen_race_identifier')
    @patch('src.data_ingestion.tpd_datasets.log_file_status')
    @patch('src.data_ingestion.tpd_datasets.process_tpd_sectionals')
    def test_process_tpd_sectionals_data_empty_directory(self, mock_process_tpd_sectionals, mock_log_file_status, mock_gen_race_identifier, mock_extract_race_date, mock_extract_course_code, mock_open, mock_isdir, mock_listdir):
        mock_conn = MagicMock()
        mock_listdir.return_value = []
        mock_isdir.return_value = False

        process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

        mock_conn.cursor().execute.assert_not_called()
        mock_log_file_status.assert_not_called()

    @patch('src.data_ingestion.tpd_datasets.os.listdir')
    @patch('src.data_ingestion.tpd_datasets.os.path.isdir')
    @patch('src.data_ingestion.tpd_datasets.open', new_callable=MagicMock)
    @patch('src.data_ingestion.tpd_datasets.extract_course_code')
    @patch('src.data_ingestion.tpd_datasets.extract_race_date')
    @patch('src.data_ingestion.tpd_datasets.gen_race_identifier')
    @patch('src.data_ingestion.tpd_datasets.log_file_status')
    @patch('src.data_ingestion.tpd_datasets.process_tpd_sectionals')
    def test_process_tpd_sectionals_data_invalid_filenames(self, mock_process_tpd_sectionals, mock_log_file_status, mock_gen_race_identifier, mock_extract_race_date, mock_extract_course_code, mock_open, mock_isdir, mock_listdir):
        mock_conn = MagicMock()
        mock_listdir.return_value = ['invalid_filename']
        mock_isdir.return_value = False

        process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

        mock_conn.cursor().execute.assert_not_called()
        mock_log_file_status.assert_called()  # log_file_status should be called for the error

    @patch('src.data_ingestion.tpd_datasets.os.listdir')
    @patch('src.data_ingestion.tpd_datasets.os.path.isdir')
    @patch('src.data_ingestion.tpd_datasets.open', new_callable=MagicMock)
    @patch('src.data_ingestion.tpd_datasets.extract_course_code')
    @patch('src.data_ingestion.tpd_datasets.extract_race_date')
    @patch('src.data_ingestion.tpd_datasets.gen_race_identifier')
    @patch('src.data_ingestion.tpd_datasets.log_file_status')
    @patch('src.data_ingestion.tpd_datasets.process_tpd_sectionals')
    def test_process_tpd_sectionals_data_invalid_json(self, mock_process_tpd_sectionals, mock_log_file_status, mock_gen_race_identifier, mock_extract_race_date, mock_extract_course_code, mock_open, mock_isdir, mock_listdir):
        mock_conn = MagicMock()
        mock_listdir.return_value = ['76202211211245']
        mock_isdir.return_value = False
        mock_open.side_effect = [MagicMock(read=MagicMock(side_effect=json.JSONDecodeError('Expecting value', 'doc', 0)))]

        mock_extract_course_code.return_value = '76'
        mock_extract_race_date.return_value = '20221121'

        process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

        mock_conn.cursor().execute.assert_not_called()
        mock_log_file_status.assert_called()  # log_file_status should be called for the error

    @patch('src.data_ingestion.tpd_datasets.os.listdir')
    @patch('src.data_ingestion.tpd_datasets.os.path.isdir')
    @patch('src.data_ingestion.tpd_datasets.open', new_callable=MagicMock)
    @patch('src.data_ingestion.tpd_datasets.extract_course_code')
    @patch('src.data_ingestion.tpd_datasets.extract_race_date')
    @patch('src.data_ingestion.tpd_datasets.gen_race_identifier')
    @patch('src.data_ingestion.tpd_datasets.log_file_status')
    @patch('src.data_ingestion.tpd_datasets.process_tpd_sectionals')
    def test_process_tpd_sectionals_data_valid_json(self, mock_process_tpd_sectionals, mock_log_file_status, mock_gen_race_identifier, mock_extract_race_date, mock_extract_course_code, mock_open, mock_isdir, mock_listdir):
        mock_conn = MagicMock()
        mock_listdir.return_value = ['76202211211245']
        mock_isdir.return_value = False
        mock_open.side_effect = [MagicMock(read=MagicMock(return_value=json.dumps({'key': 'value'})))]

        mock_extract_course_code.return_value = '76'
        mock_extract_race_date.return_value = '20221121'
        mock_gen_race_identifier.return_value = '76_20221121_1'

        process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

        mock_conn.cursor().execute.assert_called()
        mock_process_tpd_sectionals.assert_called()  # process_tpd_sectionals should be called
        mock_log_file_status.assert_called()  # log_file_status should be called for success

if __name__ == '__main__':
    unittest.main()