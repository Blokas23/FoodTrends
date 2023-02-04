from preprocessing.data_preprocessing import Data_prep
from secrets import db_ip, db_password
import pandas as pd


class Review_based_trends(Data_prep):
    """This class analysis the data obtained by the places API.

    Parameters
    ----------
    Data_prep : Class
        This class will mostly inherit its functions and attributes from the
        class Database_connection. Hence, if you have any questions regarding
        the setup please consume the docstring of the Database_connection class.
    """

    def data(self, table_name: str, database_name: str) -> pd.DataFrame:
        return self.remove_duplicate_rows(table_name=table_name,
                                          database_name=database_name)


# Testing area
test = Review_based_trends(db_ip=db_ip, db_password=db_password)
test_data = test.data(table_name="places", database_name="foodtrends")

test_data
