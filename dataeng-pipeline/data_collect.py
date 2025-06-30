import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer


class PesRegionList:
    def __init__(self):
        """
        Initializes JSON PES region list as Dict or List
        """

        self.pes_region_json = self._set_pes_region_json()


    def _set_pes_region_json(self):
        """
        SET and GET the PES region list from the PV_Live API
        
        Returns
        -------
        dict | list
            Dict or list representation of JSON
        """

        url = 'https://api.pvlive.uk/pvlive/api/v4/pes_list'
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
        }
        response = requests.get(url, headers=headers)

        return response.json()


    def get_pes_region_json(self):
        """
        GET PES region list
        
        Returns
        -------
        dict | list
            Dict or list representation of JSON
        """

        return self.pes_region_json



class JsonToDataFrame:
    def __init__(self, json_data: list | dict):
        """
        Convert JSON to Dafaframe
        
        Parameters
    ----------
        json_data: dict or list
            Dict or List representation of JSON
        """

        self.df = self._set_json_to_dataframe(json_data)


    def _set_json_to_dataframe(self, data: list | dict):
        """
        SET Dataframe
        
        Parameters
    ----------
        json_data: dict or list
            Dict or List representation of JSON
            
        Returns
        -------
        pandas DataFrame
            DataFrame representation of JSON
        """
        
        return pd.DataFrame(data.get('data'), columns = ['pes_id', 'pes_name', 'pes_longname'])


    def get_dataframe(self):
        """
        GET Dataframe
        
        Returns
        -------
        pandas DataFrame
            DataFrame representation of JSON
        """

        return self.df



class DataFrameToDb:
    def __init__(self, data_frame: pd.DataFrame):
        """
        Stores Dataframe in PostgresSQL DB
        
        Parameters
    ----------
        data_frame: pandas DataFrame
            DataFrame representation of JSON
        """

        self.df = data_frame


    def dataframe_to_sql_db(self):
        """
        Stores DataFrame to SQL database table
        """

        engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/dataengpipeline')
        with engine.begin() as connection:
            self.df.to_sql(name='pesregion', con=connection, if_exists='replace', index=False, dtype={'pes_id': Integer()})


tester = PesRegionList()
tester_json = tester.get_pes_region_json()
print(tester_json)

tester2 = JsonToDataFrame(tester_json)
tester_dataframe = tester2.get_dataframe()
print(tester_dataframe)

tester3 = DataFrameToDb(tester_dataframe)
tester3.dataframe_to_sql_db()


# r = requests.get('https://api.pvlive.uk/pvlive/api/v4/pes/23?start=2025-01-01&end=2025-06-01&extra_fields=installedcapacity_mwp', headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate',})

# print('r.text')
# print(r.json())