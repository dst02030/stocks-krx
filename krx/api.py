import logging
import json
import requests
import os


import numpy as np
import pandas as pd

from datetime import datetime

logger = logging.getLogger(__name__)



class Krx_base_api:
    
    def __init__(self, auth_key, max_rows = 100000, data_type = "json"):
        self.title_url = "https://data-dbg.krx.co.kr/svc/apis"
        self.max_rows = max_rows if max_rows else 100000
        self.data_type = data_type if data_type else 'json'
        self.headers = {
        'AUTH_KEY': auth_key
        }


    def get_api_data(self, sub_url, detail_url, params = {}, rename = None, _ts = datetime.astimezone(datetime.now())):
        params = [f"{key}={val}" for key, val in params.items()]
        params_url = "&".join(params)
        res = requests.get(f"{self.title_url}/{sub_url}/{detail_url}?{params_url}", headers = self.headers)

        if not res.ok:
            raise Exception(res.text)

        data = pd.DataFrame(json.loads(res.text)['OutBlock_1'])
        data['_ts'] = os.environ['_ts']

        if rename:
            rename = {key: val for key, val in rename.items() if key in data.columns}
            data.rename(columns = rename, inplace = True)
        
        return data



class Krx_main_api(Krx_base_api):
    def __init__(self, auth_key):
        logger.info(f"### idx api is initialized! ###")
        super().__init__(auth_key)


    def get_data(self, sub_url, detail_url, date, rename = None, _ts = datetime.now(), skip_weekend = True):
        logger.info(f"### {sub_url}.{detail_url} data in {date} call starts! ###")
        
        if (skip_weekend) & (date.weekday() > 4):
            logger.info(f"Date {date} is weekend. Return empty dataframe.")
            return pd.DataFrame()

        data = super().get_api_data(sub_url, detail_url, {"basDd": date.strftime('%Y%m%d')}, rename)
        
        if data.shape[0] == 0:
            logger.warning("data from api is empty.")
            return data

        data.replace('-', np.nan, inplace=True)
        data.replace('', np.nan, inplace = True)

        if '대비' in data.columns:
            data['대비'] = data['대비'].astype(str).str.replace(",",  "")
        
        if '기준일자' not in data.columns:
            data['기준일자'] = date
        
        return data
