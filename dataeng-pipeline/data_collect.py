import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer
import pymongo
import datetime
import multiprocessing as mp
import pprint # TODO: remove post testing



class PesRegionList:
    URL = 'https://api.pvlive.uk/pvlive/api/v4/pes_list'
    
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

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
        }
        response = requests.get(PesRegionList.URL, headers=headers)

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



class DataFrameToSqlDb:
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


class JsonToNoSqlDb:
    def __init__(self, data: list | dict):
        """
        Stores JSON in MongoDB DB
        
        Parameters
    ----------
        json_data: dict or list
            Dict or List representation of JSON
        """

        self.json_data = data


    def json_to_no_sql_db(self):
        """
        Stores JSON to MongoDB collection
        """

        self.json_data['date_collected'] = datetime.datetime.now(tz=datetime.timezone.utc)

        client = pymongo.MongoClient("localhost", 27017)
        db = client.dataengpipeline
        collection = db.pesregionlist
        collection.insert_one(self.json_data)


class PvGeneration:
    def __init__(self):
        self.pes_region_id_list = self.get_pes_region_ids()

    def get_pes_region_ids(self):
        client = pymongo.MongoClient("localhost", 27017)
        db = client.dataengpipeline
        collection = db.pesregionlist
        document = collection.find_one()
        document_data = document.get('data')
        pes_region_id_list = list(x[0] for x in document_data if x[0] > 0)
        
        return pes_region_id_list
    
    def _get_region_pv_data(self, pes_id: int):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
        }
        
        try:
            response = requests.get(f'https://api.pvlive.uk/pvlive/api/v4/pes/{pes_id}?start=2025-06-01&end=2025-06-30&extra_fields=installedcapacity_mwp', headers=headers)
        except Exception as e:
            return {}
        
        if response != None and response.status_code == 200:
            jsonResponse = response.json()
            return jsonResponse
        
        return {}
    
    def pv_data_to_no_sql_db(self):
        pv_data_list = []

        with mp.Pool(5) as p:
            pv_data_list.append((p.map(self._get_region_pv_data, self.pes_region_id_list)))

        client = pymongo.MongoClient("localhost", 27017)
        db = client.dataengpipeline
        collection = db.ukpvjune
        collection.insert_many(pv_data_list[0])


tester5 = PvGeneration()
pprint.pprint(tester5.pv_data_to_no_sql_db())


# TODO: proper tests!

# tester = PesRegionList()
# tester_json = tester.get_pes_region_json()
# print(tester_json)

# tester2 = JsonToDataFrame(tester_json)
# tester_dataframe = tester2.get_dataframe()
# print(tester_dataframe)

# tester3 = DataFrameToSqlDb(tester_dataframe)
# tester3.dataframe_to_sql_db()

# tester4 = JsonToNoSqlDb(tester_json)
# tester4.json_to_no_sql_db()