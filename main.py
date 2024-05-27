import logging
import os
import sys

import pandas as pd

from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


from krx.api import Krx_main_api
from krx.processing import upload_func
from krx.utils import get_jinja_yaml_conf, create_db_engine, Postgres_connect

def main():
    os.chdir(os.path.dirname(__file__))
    sub_url = sys.argv[1]
    conf = get_jinja_yaml_conf('./conf/api.yml', './conf/logging.yml')
    api_start_date = datetime.strptime(conf['api_start_date'], '%Y-%m-%d').date()
    end_date = datetime.now().date() 


    # logger 설정
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=eval(conf['logging']['level']),
        format=conf['logging']['format'],
        handlers = [TimedRotatingFileHandler(filename =  conf['logging']['file_name'],
                                    when=conf['logging']['when'],
                                    interval=conf['logging']['interval'],
                                    backupCount=conf['logging']['backupCount']), logging.StreamHandler()]
                    )



    engine = create_db_engine(os.environ)
    postgres_conn = Postgres_connect(engine)


    krx_api = Krx_main_api(auth_key = os.environ['auth_key'])
    for data_name in conf[sub_url].keys():
        start_date = api_start_date
        if 'start_date' in conf[sub_url][data_name]:
            start_date = conf[sub_url][data_name]['start_date']
        upload_func(sub_url,
                data_name, 
               krx_api, 
               postgres_conn, 
               conf, 
               start_date, 
               _ts = os.environ['_ts'], 
               skip_weekend = conf[sub_url][data_name]['skip_weekend']) 
        
if __name__ == "__main__":
    main()