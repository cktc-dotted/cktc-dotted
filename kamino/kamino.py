"""Main module."""

# Upload Daily Files to Adapter

# Set up
import ast
import json
import os
import re
from datetime import datetime, timedelta
from math import isnan

import chardet
import pandas as pd
from dateutil.relativedelta import relativedelta
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine

home = os.path.expanduser("~")

load_dotenv(find_dotenv())
daily_reports_pth = os.getenv('DAILY_REPORTS_PTH')
conn_str = 'postgresql://{}:{}@{}'.format(os.getenv('RDS_DB_USER'),
                                          os.getenv('RDS_DB_PWD'),
                                          os.getenv('RDS_DB'))
engine = create_engine(conn_str)
# engine = create_engine(conn_str, connect_args={'options': '-csearch_path={}'.format('airtable')})

p_daily_date = re.compile(r'Daily_Report\/(\d{8})')
print('user:', os.getenv('RDS_DB_USER'))
# read config file for 2GoTrade Data
df_config_2go = pd.read_csv('2godata.csv', dtype=str)
file_list = df_config_2go.to_dict('records')


def get_csv_and_save(fpath, dt_str, cols_uniq, db_tbl_raw, cols_incl):
    """Get 2GoTrade report data and upload to database

    Args:
        fpath (str): full file path and name
        dt_str (str): date string YYYYMMDD
        cols_uniq (list): list of columns from source data to be combined to create a unique ID for this particular data
        db_tbl_raw (str): database table name to store raw data
        cols_incl (list): list of columns to include in database table on save

    Returns:
        dataframe: dataframe of data sent to database
    """

    # print('!!! Working:', fpath, 'in')

    file_raw = open(fpath, "rb").read()
    chardet_result = chardet.detect(file_raw)

    df_i = pd.read_csv(
        fpath, sep='\t', encoding=chardet_result['encoding'], index_col=False)
    df_i.columns = df_i.columns.str.replace(
        '\W', '_').str.lower()  # Clean column names

    df_i['dt_str'] = dt_str
    df_i['ts_utc'] = pd.Timestamp.utcnow()

    # Create uid_str
    df_i['uid_str'] = df_i[cols_uniq].apply(
        lambda row: '_'.join(row.values.astype(str)), axis=1)
    #     df_i['uid_str'].is_unique # check uid_str is unique

    if cols_incl:
        # print('Override Cols')
        cols_incl = cols_incl
    else:
        # print('Standard Cols')
        cols_incl = list(df_i.columns.values)

    df_i[cols_incl].to_sql(
        db_tbl_raw, engine, if_exists='append')  # Save to DB

    return df_i[cols_incl]


# Main
cutoff = datetime.now() + relativedelta(days=-7)
cutoff_dt_str = int(cutoff.strftime('%Y%m%d'))

for subdir, dirs, files in os.walk(daily_reports_pth):
    for file in files:
        filepath = subdir + os.sep + file
        # print('1', file)
        # print('2', filepath)
        for each in file_list:
            if file.lower() == each['fname'].lower():
                m = p_daily_date.search(str(subdir))
                daily_date = m.group(1)
                # print (daily_date)
                # print (filepath, datetime.fromtimestamp(os.path.getmtime(filepath)))
                fname = file
                dt_str = daily_date
                cols_uniq = ast.literal_eval(each['cols_uniq'])
                cols_incl = ast.literal_eval(each['cols_incl'])
                db_tbl_raw = each['db_tbl_raw']

                # cutoff_dt = 20210907 # set earliest date to process (so it doesn't process everything)

                if fname and dt_str and cols_uniq and db_tbl_raw and int(dt_str) > cutoff_dt_str:
                    print('Getting:', dt_str, fname)
                    if cols_incl:
                        # print('Has cols_incl')
                        get_csv_and_save(filepath, dt_str,
                                         cols_uniq, db_tbl_raw, cols_incl)
                    else:
                        # print('Has no cols_incl')
                        get_csv_and_save(filepath, dt_str,
                                         cols_uniq, db_tbl_raw, cols_incl)
