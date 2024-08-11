import os

import pandas as pd
from abc import ABC, abstractmethod
from datetime import date


class BaseETL(ABC):
    @abstractmethod
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        """
        Method to reach the source and extract the data as an object
        """
        pass

    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Method for transforming extracted data.
        """
        pass

    def load(
        self,
        data: pd.DataFrame,
        context: str,
        extraction_date: date = None,
        file_name: str = None,
    ):
        """
        Method for loading the data, default: writing to a partitioned parquet file
        Args:
          data: Pandas DataFrame containing the data to be written.
          context: Context of the extraction, it will be used to generate the path
          extraction_date: The date of the extraction, if left empty the data will be written to the root directory
          file_name: Name for the file to be written, if left empty the file will be named: <context.parquet>
        """
        output_dir = (
            f'context={context}/_extraction_date={extraction_date.strftime("%Y-%m-%d")}'
        )
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, file_name)
        data.to_parquet(file_path, index=False)

    def run(
        self,
        *args,
        context: str,
        extraction_date: date = None,
        file_name: str = None,
    ):
        """
        Method for running default ETL process.
        Args:
          *args: Variable length arguments specific to the extraction process.
          context: Context of the extraction
          extraction_date: The processing date
          file_name: Name for the file to be written

        Returns:
          None
        """
        extracted_data = self.extract(*args)
        transformed_data = self.transform(extracted_data)
        self.load(
            transformed_data,
            context=context,
            extraction_date=extraction_date,
            file_name=file_name,
        )
