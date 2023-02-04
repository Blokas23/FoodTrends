import mysql.connector
from mysql.connector.constants import ClientFlag
import pandas as pd
from sqlalchemy import create_engine
from logger import logger


class Database_connection():
    """This class creates a connection to the database and offers some
    functionalities regarding this connection like create a database,
    write and retrieve data from/to a selected database.
    """

    def __init__(self, db_password: str, db_ip: str):
        """This function instantiates the class Database_connection. It creates
        an dictionary to store all the needed information.

        Parameters
        ----------
        db_password : str
            Password for the database.
        db_ip : str
            The public IP for the database.
        """
        self.config = {
            'user': 'root',
            'password': db_password,
            'host': db_ip,
            'client_flags': [ClientFlag.SSL]  # ,
            # 'ssl_ca': 'database/ssl/server-ca.pem',
            # 'ssl_cert': 'database/ssl/client-cert.pem',
            # 'ssl_key': 'database/ssl/client-key.pem'
        }
        logger.info("Database_connection class initialized")

    def connector(self, **kwargs):
        """This function connects to the database.

        Returns
        -------
        Connection
            It returns the connection object for the database.
        """
        connection = mysql.connector.connect(**self.config, **kwargs)
        logger.info("Created connection to database on GCP")
        return connection

    def create_database(self, database_name: str):
        """This creates a connection to the database, leveraging the connector
        function and creates a new database.

        Parameters
        ----------
        database_name : str
            The name of the database which should be created
        """
        connection = self.connector()

        cursor = connection.cursor()
        try:
            cursor.execute('CREATE DATABASE ' + database_name)
            connection.close()
        except mysql.connector.errors.DatabaseError:
            logger.warn("A database with that name already exists")

    def add_data(self, dataframe: pd.DataFrame, table_name: str,
                 database_name: str, if_exists_parameter= "append"):
        """This function writes data to the designated database as well as its
        table. If the table exists, the data will be appended.

        Parameters
        ----------
        dataframe : pd.DataFrame
            The dataframe which should be appended or created.
        table_name : str
            The name of the table.
        database_name : str
            The name of the database where the table should be created or the
            data appened to.
        if_exists_parameter : str
            A parameter which we can set to either append or replace our table.
            Default value is "append". 
        """
        connection = create_engine(
            f"mysql+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}/{database_name}")

        dataframe.to_sql(name=table_name, con=connection,
                         if_exists=if_exists_parameter, index=False)
        logger.info("The data was inputted into the defined database/table")

    def read_data(self, table_name: str, database_name: str):
        connection = create_engine(
            f"mysql+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}/{database_name}")
        data = pd.read_sql(f"SELECT * FROM {table_name} ", con=connection)
        return data
