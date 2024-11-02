import os
import json
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from src.data_ingestion.tpd_datasets import process_tpd_sectionals_data

@pytest.fixture
def mock_conn():
    conn = MagicMock()
    conn.cursor.return_value = MagicMock()
    return conn

@pytest.fixture
def mock_filesystem():
    with patch('os.listdir') as mock_listdir, \
         patch('os.path.isdir') as mock_isdir, \
         patch('builtins.open', create=True) as mock_open:
        yield mock_listdir, mock_isdir, mock_open

@pytest.fixture
def mock_utils():
    with patch('src.data_ingestion.tpd_datasets.extract_course_code') as mock_extract_course_code, \
         patch('src.data_ingestion.tpd_datasets.extract_race_date') as mock_extract_race_date, \
         patch('src.data_ingestion.tpd_datasets.gen_race_identifier') as mock_gen_race_identifier, \
         patch('src.data_ingestion.tpd_datasets.log_file_status') as mock_log_file_status, \
         patch('src.data_ingestion.tpd_datasets.process_tpd_sectionals') as mock_process_tpd_sectionals:
        yield mock_extract_course_code, mock_extract_race_date, mock_gen_race_identifier, mock_log_file_status, mock_process_tpd_sectionals

def test_process_tpd_sectionals_data_empty_directory(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = []
    mock_isdir.return_value = False

    process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

    mock_conn.cursor().execute.assert_not_called()

def test_process_tpd_sectionals_data_invalid_filenames(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = ['invalid_filename']
    mock_isdir.return_value = False

    mock_extract_course_code, mock_extract_race_date, mock_gen_race_identifier, mock_log_file_status, mock_process_tpd_sectionals = mock_utils
    mock_extract_course_code.side_effect = ValueError("Invalid filename")
    mock_extract_race_date.side_effect = ValueError("Invalid filename")

    process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

    mock_log_file_status.assert_not_called()
    mock_conn.cursor().execute.assert_not_called()

def test_process_tpd_sectionals_data_invalid_json(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = ['valid_filename']
    mock_isdir.return_value = False

    mock_extract_course_code, mock_extract_race_date, mock_gen_race_identifier, mock_log_file_status, mock_process_tpd_sectionals = mock_utils
    mock_extract_course_code.return_value = '76'
    mock_extract_race_date.return_value = '20221121'
    mock_gen_race_identifier.return_value = '76202211231'

    mock_open.side_effect = [MagicMock(read=MagicMock(side_effect=json.JSONDecodeError("Expecting value", "", 0)))]

    process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

    mock_log_file_status.assert_called()
    mock_conn.cursor().execute.assert_not_called()

def test_process_tpd_sectionals_data_valid_json(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = ['76202211211245', '76202211211312']
    mock_isdir.return_value = False

    mock_extract_course_code, mock_extract_race_date, mock_gen_race_identifier, mock_log_file_status, mock_process_tpd_sectionals = mock_utils
    mock_extract_course_code.side_effect = ['76', '76']
    mock_extract_race_date.side_effect = ['20221121', '20221121']
    mock_gen_race_identifier.side_effect = ['76202211231', '76202211232']

    mock_open.side_effect = [
        MagicMock(read=MagicMock(return_value=json.dumps({"key": "value"}))),
        MagicMock(read=MagicMock(return_value=json.dumps({"key": "value"})))
    ]

    process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

    assert mock_process_tpd_sectionals.call_count == 2
    mock_log_file_status.assert_called()
    mock_conn.cursor().execute.assert_called()

def test_process_tpd_sectionals_data_invalid_filenames(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = ['invalid_filename']
    mock_isdir.return_value = False

    process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

    mock_conn.cursor().execute.assert_not_called()
    mock_utils[3].assert_called()  # log_file_status should be called for the error

def test_process_tpd_sectionals_data_invalid_json(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = ['76202211211245']
    mock_isdir.return_value = False
    mock_open.side_effect = [MagicMock(read=MagicMock(side_effect=json.JSONDecodeError('Expecting value', 'doc', 0)))]

    mock_utils[0].return_value = '76'
    mock_utils[1].return_value = '20221121'

    process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

    mock_conn.cursor().execute.assert_not_called()
    mock_utils[3].assert_called()  # log_file_status should be called for the error

def test_process_tpd_sectionals_data_valid_json(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = ['76202211211245']
    mock_isdir.return_value = False
    mock_open.side_effect = [MagicMock(read=MagicMock(return_value=json.dumps({'key': 'value'})))]

    mock_utils[0].return_value = '76'
    mock_utils[1].return_value = '20221121'
    mock_utils[2].return_value = '76_20221121_1'

    process_tpd_sectionals_data(mock_conn, 'dummy_path', 'dummy_error_log', set())

    mock_conn.cursor().execute.assert_called()
    mock_utils[4].assert_called()  # process_tpd_sectionals should be called
    mock_utils[3].assert_called()  # log_file_status should be called for success