import os
import json
import pytest
from unittest import mock
from unittest.mock import MagicMock, patch
from src.data_ingestion.tpd_datasets import process_tpd_data

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

def test_process_tpd_data_invalid_json(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = ['ST202409021235']
    mock_isdir.return_value = False
    mock_open.side_effect = [MagicMock(read=MagicMock(side_effect=json.JSONDecodeError('Expecting value', 'doc', 0)))]

    mock_extract_course_code, mock_extract_race_date, mock_gen_race_identifier, mock_log_file_status, mock_process_tpd_sectionals = mock_utils
    mock_extract_course_code.return_value = 'ST'
    mock_extract_race_date.return_value = '20240902'

    print("Calling process_tpd_data for invalid JSON")
    process_tpd_data(mock_conn, 'dummy_path', 'dummy_error_log', set(), data_type="sectionals")
    print("Finished calling process_tpd_data for invalid JSON")

    mock_log_file_status.assert_called()  # log_file_status should be called for the error
    mock_conn.cursor().execute.assert_not_called()

def test_process_tpd_data_extract_metadata_and_process_sectionals(mock_conn, mock_filesystem, mock_utils):
    mock_listdir, mock_isdir, mock_open = mock_filesystem
    mock_listdir.return_value = ['ST202409021235', 'ST202409021307', 'ST202409021340']
    mock_isdir.return_value = False
    mock_open.side_effect = [MagicMock(read=MagicMock(return_value=json.dumps({'key': 'value'})))]

    mock_extract_course_code, mock_extract_race_date, mock_gen_race_identifier, mock_log_file_status, mock_process_tpd_sectionals = mock_utils
    mock_extract_course_code.return_value = 'ST'
    mock_extract_race_date.return_value = '20240902'
    mock_gen_race_identifier.return_value = 'ST_20240902_1'

    print("Calling process_tpd_data for valid JSON")
    process_tpd_data(mock_conn, 'dummy_path', 'dummy_error_log', set(), data_type="sectionals")
    print("Finished calling process_tpd_data for valid JSON")

    print("Checking if process_tpd_sectionals was called")
    mock_process_tpd_sectionals.assert_called_with(
        mock_conn,
        {'key': 'value'},
        'ST',
        '20240902',
        1,
        'ST_20240902_1',
        'dummy_path',
        set(),
        mock.ANY
    )
    print("process_tpd_sectionals was called")

    print("Checking if log_file_status was called")
    mock_log_file_status.assert_called_with(mock_conn, 'ST202409021235', mock.ANY, 'processed')
    print("log_file_status was called")

    print("Checking if execute was called")
    mock_conn.cursor().execute.assert_called()
    print("execute was called")