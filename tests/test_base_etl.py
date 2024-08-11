import unittest
from datetime import date
from unittest.mock import patch
import pandas as pd

# Enabling test modules
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import base_etl


class TestBaseETL(unittest.TestCase):
    def setUp(self):
        self.data = pd.json_normalize([{"name": "fake_name"}])
        self.context = "fake_context"
        self.extraction_date = date(2024, 1, 1)
        self.file_name = "fake_file.parquet"

    @patch("os.makedirs")
    @patch("pandas.DataFrame.to_parquet")
    def test_load_with_all_params(self, mock_to_parquet, mock_makedirs):
        base_etl.BaseETL.load(
            self,
            data=self.data,
            context=self.context,
            extraction_date=self.extraction_date,
            file_name=self.file_name,
        )

        expected_dir = f"context={self.context}/_extraction_date={self.extraction_date.strftime('%Y-%m-%d')}"
        expected_file_path = os.path.join(expected_dir, self.file_name)

        mock_makedirs.assert_called_once_with(expected_dir, exist_ok=True)
        mock_to_parquet.assert_called_once_with(expected_file_path, index=False)


if __name__ == "__main__":
    unittest.main()
