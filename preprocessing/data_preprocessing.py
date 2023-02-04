from database.database_connection import Database_connection
import pandas as pd


class Data_prep(Database_connection):
    """This class will mostly inherit its functions and attributes from the
    class Database_connection. Hence, if you have any questions regarding
    the setup please consume the docstring of the Database_connection class.

    Parameters
    ----------
    Database_connection : class
        This class takes the connection params, creates a connection
        to the database and lets us access it. 
    """

    def remove_duplicate_rows(self, table_name: str, database_name: str) -> pd.DataFrame:
        """This function reads the data from the given table in the specified
        database and removes all duplicates in it.

        Parameters
        ----------
        table_name : str
            The name of the table which shall be accessed.
        database_name : str
            The name of the database wherein the tables lies.

        Returns
        -------
        pd.DataFrame 
            The dataframe with removed duplicate rows, read for analysis.
        """
        data = self.read_data(table_name=table_name,
                              database_name=database_name)
        return data.drop_duplicates()
