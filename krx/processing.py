import logging

import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def upload_func(sub_url, data_name, api_client, db_client, conf, start_date, end_date = datetime.now(), _ts = datetime.now(), skip_weekend = True):
    logger.info(f"{sub_url}.{data_name} upload starts!")
    db_latest_date = db_client.get_maxmin_col(conf['schema_name'], conf[sub_url][data_name]['table_name'], 
                conf[sub_url][data_name]['date_col'], 
                where = [f"{conf[sub_url][data_name]['class_col']} = '{conf[sub_url][data_name]['class_name']}'"])[0]

    db_latest_date = db_latest_date if db_latest_date else start_date - timedelta(days = 1)
    start_date = max(start_date, db_latest_date + timedelta(days = 1))

    for basdd in pd.date_range(start_date, end_date):
        data = api_client.get_data(sub_url, conf[sub_url][data_name]['detail_url'], basdd.date(), 
                                       rename = conf[sub_url][data_name]['rename'], _ts = _ts, skip_weekend = skip_weekend)
        
        
        if (data.shape[0] > 0) & (sub_url in ['etp', 'drv', 'gen']):
            data['상장타입'] = conf[sub_url][data_name]['class_name'].upper()
            
        if '종가' in data.columns:
            data.dropna(subset=['종가'], axis = 0, inplace = True)
            
        if '기초지수자산종가' in data.columns:
            data['기초지수자산종가'] = data['기초지수자산종가'].astype(str).str.replace(",",  "")

        if data.shape[0] > 0:
            
            data.to_sql(conf[sub_url][data_name]['table_name'], schema = conf['schema_name'], con = db_client.get_engine(), index = False, if_exists = "append")
            