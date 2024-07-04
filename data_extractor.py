import datetime
import logging
import requests
import time
from typing import Tuple


import pandas as pd
from pandas import errors
import numpy as np
import mysql.connector
from mysql.connector import errorcode
import sqlalchemy
from sqlalchemy import create_engine


class DataExtractor():
    def __init__(self, configs) -> None:
        self.configs = configs
        logging.basicConfig(level=logging.INFO, 
                                          filename=f'logs/{str(datetime.datetime.now().date())}_log.log',
                                          filemode="w", 
                                          format="%(asctime)s %(levelname)s %(message)s")
        self.logger = logging.getLogger('main_logger')

        self.db, self.sqlach_engine = self._connect_db()
        self.students_dict = self._get_student_dict()

        
    
    def _connect_db(self):
        """
        This internal method connects to the MySQL database that stores survey participants' information 
        and the subsequent AQI calculations 

        Returns:
        mysql connection object to commit the transaction to the DB permanently
        mysqlalchemy engine to read and write data to the DB 

        """
        creds = {'user': self.configs['credentials']['database']['user'],
                 'pwd': self.configs['credentials']['database']['pwd'],
                 'host': self.configs['credentials']['database']['host'],
                 'port': self.configs['credentials']['database']['port'],
                 'db': self.configs['credentials']['database']['database']}
        
        # Connect to MySQL DB and test connection
        try:
            mydb = mysql.connector.connect(
                    host=creds['host'],
                    user=creds['user'],
                    password=creds['pwd'],
                    database=creds['db'],
                    port=creds['port']
                )
            self.logger.info("Successfully connected to DB")

                
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                # print("Something is wrong with your user name or password")
                self.logger.exception("ACCESS_DENIED_ERROR: invalid username or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                # print("Database does not exist")
                self.logger.exception("BAD_DB_ERROR")
            else:
                # print(err)
                self.logger.error(err)
        

        # MySQL conection string.
        connstr = 'mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}'

        sqlach_engine = create_engine(connstr.format(**creds))


        return mydb, sqlach_engine
    

    
    def _get_student_dict(self) -> dict:
        """
        This internal method queries the most current list survey participants and their registered coordinates
        from HUPH's DB 

        Returns:
        A dictionary of survey participants containing their latest registered coordinates
        """
        students_df = pd.read_sql(self.configs['sql_queries']['students'],
                                  con=self.sqlach_engine)
        
        if len(students_df) < 1:
            self.logger.error(errors.EmptyDataError('DataFrame is empty'))
            raise errors.EmptyDataError('DataFrame is empty')

        students_df = students_df[students_df['DT'].dt.date == students_df['DT'].dt.date.max()]
        
        return students_df.set_index('STUDENT_ID').T.to_dict('dict')
    

    def _extract_from_owm(self, student_id:int, request_type: str) -> dict:

        """
        This internal method uses a pair of latitude and longitude to extract from OWM either 
        1) the current air pollution concentrations and 
        2) the air pollution forecasts 

        Parameters:
        student_id: The survey participant's student id
        request_type: The type of API request to OWM, can be either 
                            1) current air pollution or 2) forecasted air pollution
        
        Returns:
        r_json: A dictionary containing the API response 

        """

        api_key = self.configs['credentials']['owm_api_key']
        lat = self.students_dict[student_id]['LAT']
        lon = self.students_dict[student_id]['LON']

        if request_type == 'current_air':
            url = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}'
        elif request_type == 'forecast_air':
            url = f'http://api.openweathermap.org/data/2.5/air_pollution/forecast?lat={lat}&lon={lon}&appid={api_key}'
        else:
            self.logger.error('Wrong request type. Should be either "current_air" pr "forecast_air"!')
            raise ValueError('Wrong request type. Should be either "current_air" pr "forecast_air"!')
        
        r = requests.get(url)
        while r.status_code == 429:
            self.logger.warning("OWM minute quota reached, delay for 60s")
            time.sleep(60)
            self.logger.info("Retrying OWM GET request")
            r = requests.get(url)

        r_json = r.json()
        r_json['STUDENT_ID'] = student_id

        return r_json
    
    @staticmethod
    def _format_concentration(df: pd.DataFrame, type: str) -> pd.DataFrame:
        """
        An internal static method which processes values of various PM 2.5 aggregations to the 
        appropriate demical points to be used to compute the corresponding AQI values
        https://www.airnow.gov/sites/default/files/2020-05/aqi-technical-assistance-document-sept2018.pdf 

        Parameters:
        df: The pandas dataframe containing the PM 2.5 aggregations
        type: The type of the aggregations contained within df. Can be either
              1) current
              2) forecast

        Returns:
        df: The processed PM 2.5 dataframe  

        """
        if type == 'current':
            df['PM25'] = np.round(df['PM25'], 1)
        if type == 'forecast':
            df['PM25_TODAY'] = np.round(df['PM25_TODAY'], 1)
            df['PM25_NEXT_DAY'] = np.round(df['PM25_NEXT_DAY'], 1)

        return df
    
    @staticmethod
    def _calculate_aqi(concentration: float) -> float:
        """
        This internal static method computes the AQI value of the input PM2.5 concentration. Uses AQI breakpoints found in:
        https://www.airnow.gov/sites/default/files/2020-05/aqi-technical-assistance-document-sept2018.pdf 

        Parameters:
        concentration: the input PM 2.5 concentration

        Returns: 
        The computed AQI value
        """
        breakpoints_pm25 = [(0, 50, 0.0, 9.0), (51, 100, 9.1, 35.4), (101, 150, 35.5, 55.4), (151, 200, 55.5, 125.4), (201, 300, 125.5, 225.4), (301, 500, 225.5 , 5000)]
        for (I_low, I_high, C_low, C_high) in breakpoints_pm25:
            if C_low <= concentration <= C_high:
                return (I_high - I_low) / (C_high - C_low) * (concentration - C_low) + I_low
            

    def get_rt(self, student_ids: list) -> pd.DataFrame:
        """
        This method takes the list of survey partcipants by their student ids and computes the corresponding current AQI.
        The results are encapsulated in a pandas dataframe

        Parameters:
        student_ids: the list of student ids of survey participants
        
        Returns:
        df_rt: The pandas dataframe containing the survey participants' current AQI values.
        """        
        dfs = []

        for id in student_ids:
            r_air = self._extract_from_owm(id, request_type='current_air')


            pm25 = [r_air['list'][0]['components']['pm2_5']]
            dt = [r_air['list'][0]['dt']]
            
            df = pd.DataFrame({'STUDENT_ID': r_air['STUDENT_ID'],
                            'LAT': r_air['coord']['lat'],
                            'LON': r_air['coord']['lon'],
                            'DT_UNIX': dt,
                            'PM25': pm25})

            dfs.append(df)

        df_rt = pd.concat(dfs, axis=0).reset_index(drop=True)

        df_rt['DT'] = pd.to_datetime(df_rt['DT_UNIX'],unit='s')
        df_rt['DT'] = df_rt['DT'].dt.tz_localize('UTC').dt.tz_convert('Asia/Ho_Chi_Minh')

        df_rt = df_rt.drop(['DT_UNIX'], axis=1)
        df_rt = df_rt[['STUDENT_ID', 'LAT', 'LON', 'DT', 'PM25']]

        df_rt = self._format_concentration(df_rt, 'current')
        df_rt['AQI_CURRENT'] = np.round(df_rt['PM25'].apply(lambda x: self._calculate_aqi(x)), 0)
        df_rt['AQI_CURRENT'] = df_rt['AQI_CURRENT'].astype(int)
        
        return df_rt


    def get_fc(self, student_ids: list) -> pd.DataFrame:
        """
        This method takes the list of survey partcipants by their student ids and computes the corresponding forecasted mean daily AQI of the current and next day.
        The results are encapsulated in a pandas dataframe

        Parameters:
        student_ids: the list of student ids of survey participants
        
        Returns:
        df_fc: The pandas dataframe containing the survey participants' forecasted and averaged AQI values.
        """      
        dfs = [] 
        for id in student_ids:

            r_forecast = self._extract_from_owm(id, request_type='forecast_air')

            pm25_today = [x['components']['pm2_5'] for x in r_forecast['list'][:23]]
            dt_today = [x['dt'] for x in r_forecast['list'][:23]]

            pm25_next_day = [x['components']['pm2_5'] for x in r_forecast['list'][23:46]]
            dt_next_day = [x['dt'] for x in r_forecast['list'][23:46]]

            df = pd.DataFrame({'STUDENT_ID': r_forecast['STUDENT_ID'],
                                'LAT': r_forecast['coord']['lat'],
                                'LON': r_forecast['coord']['lon'],
                                'DT_TODAY': dt_today,
                                'PM25_TODAY': pm25_today,
                                'DT_NEXT_DAY': dt_next_day,
                                'PM25_NEXT_DAY': pm25_next_day})
            
            dfs.append(df)

        df_fc = pd.concat(dfs, axis=0).reset_index(drop=True)

        df_fc['DT_TODAY'] = pd.to_datetime(df_fc['DT_TODAY'],unit='s')
        df_fc['DT_TODAY'] = df_fc['DT_TODAY'].dt.tz_localize('UTC').dt.tz_convert('Asia/Ho_Chi_Minh')
        df_fc['DT_NEXT_DAY'] = pd.to_datetime(df_fc['DT_NEXT_DAY'],unit='s')
        df_fc['DT_NEXT_DAY'] = df_fc['DT_NEXT_DAY'].dt.tz_localize('UTC').dt.tz_convert('Asia/Ho_Chi_Minh')

        df_fc = self._format_concentration(df_fc, 'forecast')
        df_fc['AQI_TODAY'] = np.round(df_fc['PM25_TODAY'].apply(lambda x: self._calculate_aqi(x)), 0)
        df_fc['AQI_NEXT_DAY'] = np.round(df_fc['PM25_NEXT_DAY'].apply(lambda x: self._calculate_aqi(x)), 0)

        df_fc = df_fc.groupby(['STUDENT_ID', 'LAT', 'LON']).agg({'AQI_TODAY': 'mean', 
                                                                   'AQI_NEXT_DAY': 'mean'}).reset_index()
        df_fc['AQI_TODAY'] = np.round(df_fc['AQI_TODAY']).astype(int)
        df_fc['AQI_NEXT_DAY'] = np.round(df_fc['AQI_NEXT_DAY']).astype(int)

        return df_fc[['STUDENT_ID', 'AQI_TODAY', 'AQI_NEXT_DAY']]