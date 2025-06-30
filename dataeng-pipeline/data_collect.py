import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer

class PesRegionList:
    def __init__(self):
        """
        Initializes JSON PES Region List
        """
        self.pes_region_json = self._set_pes_region_json()
        self.df = None


    def _set_pes_region_json(self):
        """
        SET the PES region list from the PV_Live API
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
        """
        return self.pes_region_json



class PesRegionListJsonToDb():
    def __init__(self, data: list | dict):
        """
        Convert JSON to Dafaframe; stores Dataframe in PostgresSQL DB
        """
        self.json_data = data
        self.df = None


    def set_json_to_dataframe(self):
        """
        SET Dataframe
        """
        self.df = pd.DataFrame(self.json_data.get('data'), columns = ['pes_id', 'pes_name', 'pes_longname'])


    def get_dataframe(self):
        """
        GET Dataframe
        """
        return self.df


    def dataframe_to_sql_db(self):
        """
        Stores DataFrame lto SQL database table
        """
        engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/dataengpipeline')
        with engine.begin() as connection:
            self.df.to_sql(name='pesregion', con=connection, if_exists='replace', index=False, dtype={'pes_id': Integer()})
        
        return True


tester = PesRegionList()
print(tester.get_pes_region_json())

tester2 = PesRegionListJsonToDb(tester.get_pes_region_json())
tester2.set_json_to_dataframe()
print(tester2.get_dataframe())
tester2.dataframe_to_sql_db()


# r = requests.get('https://api.pvlive.uk/pvlive/api/v4/pes/23?start=2025-01-01&end=2025-06-01&extra_fields=installedcapacity_mwp', headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Accept-Encoding': 'gzip, deflate',})

# print('r.text')
# print(r.json())