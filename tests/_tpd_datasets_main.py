import unittest
from unittest.mock import MagicMock, patch
import json
from src.data_ingestion.tpd_datasets import process_tpd_data

class TestProcessTPDData(unittest.TestCase):

    @patch('src.data_ingestion.tpd_datasets.os.listdir')
    @patch('src.data_ingestion.tpd_datasets.os.path.isdir')
    @patch('src.data_ingestion.tpd_datasets.open', new_callable=MagicMock)
    @patch('src.data_ingestion.tpd_datasets.extract_course_code')
    @patch('src.data_ingestion.tpd_datasets.extract_race_date')
    @patch('src.data_ingestion.tpd_datasets.gen_race_identifier')
    @patch('src.data_ingestion.tpd_datasets.log_file_status')
    def test_process_tpd_data_invalid_json(self, mock_log_file_status, mock_gen_race_identifier, mock_extract_race_date, mock_extract_course_code, mock_open, mock_isdir, mock_listdir):
        mock_conn = MagicMock()
        mock_listdir.return_value = ['valid_filename']
        mock_isdir.return_value = False
        mock_open.side_effect = [MagicMock(read=MagicMock(side_effect=json.JSONDecodeError('Expecting value', 'doc', 0)))]

        mock_extract_course_code.return_value = '76'
        mock_extract_race_date.return_value = '20221121'

        process_tpd_data(mock_conn, 'dummy_path', 'dummy_error_log', set(), data_type="sectionals")

        mock_log_file_status.assert_called()  # log_file_status should be called for the error

if __name__ == '__main__':
    unittest.main()