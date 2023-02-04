from preprocessing.data_preprocessing import Data_prep
import pandas as pd
from secrets import db_ip, db_password


class Word_based_trends(Data_prep):

    def data(self, table_name: str, database_name: str):
        """This function retrieves the cleaned data

        Parameters
        ----------
        table_name : str
            The name of the table which shall be accessed.
        database_name : str
            The name of the database wherein the tables lies.
        """
        self.data_ = self.remove_duplicate_rows(table_name=table_name,
                                                database_name=database_name)


# Test
reddits = Word_based_trends(db_ip=db_ip, db_password=db_password)
reddits.data("reddits", "foodtrends")
reddits.data_
