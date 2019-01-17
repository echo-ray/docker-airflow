from airflow import DAG
import pandas as pd
import numpy as np
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import logging


# 爬取产生数据，存入数据库 >> 取出原始数据，清洗处理至指定格式 >> 存入正式数据库
#
# 也可能还需要一个校验的环节？
# 产生数据，存入原始数据库
#       >> 取出原始数据，处理至指定格式
#               >> 校验对比     >> 存入正式数据库
#       >> 取出聚源数据，处理至相同格式

'''
['index_quot_day', # 每日指数
 'company_announcement_information_ori',
 'company_announcement_information_TS',
 'violation_handling',  # 违规处罚
 'ownership_hierarchy', 
 'cominv_interactive',  # 投资者互动
 'institution_shares',  # 机构持股
 'financial_target_reportdata',
 'hold_shares_total',
 'equity_hold_company', 
 'indicator_change_description',  # 指标变动
 'DuPont_analysis',  # 杜邦分析
]
'''

def raw_data(n_row):
    # get some test dataset，后面也可以是爬虫产生的
    df = pd.DataFrame(
        index = range(1, n_row+1),
        data = np.random.randint(0,111,size=(n_row, 4)),
        columns=['aaa','bbb','ccc', 'dd'])
    df['time'] = pd.period_range(start='2018-01-02', periods=n_row)
    logging.info('data generated')
    return df

def data_to_mysql(data, table_name, con=None):
    # put data to mysql
    if not con:
        # logging.info('data_to_mysql need a mysql connection')
        con = create_engine(
            'mysql://rootb:3x870OV649AMSn*@' +\
            '14.152.49.155:8998/zyr?charset=gbk').connect()
    df.to_sql(
        table_name, con, if_exists='append', index=False,
        chunksize=np.ceil(df.size/1000)*1000)
    logging.info('data has saved to mysql')
    return True

def gen_raw_data(n_rows):
    data = raw_data(n_rows)
    data_to_mysql(data, 'test_db_1')
    return True


def compare_data(db, table, raw_db=None, raw_table=None):
    # 对比拿到最后一个 id
    last_of_raw = pd.read_sql_query(
        f'SELECT * FROM {raw_table} ORDER BY id DESC LIMIT 1',
        con=raw_db)['id'][0]

    try:
        last = pd.read_sql_query(
            f'SELECT * FROM {table} ORDER BY id DESC LIMIT 10',
            con=db)['id'][0]
        if last > last_of_raw:
            raise Exception("异常！数据库末尾id大于原始数据库末尾id")
    except ProgrammingError:
        last = 0
    logging.info('comparing data sets')
    return (last, last_of_raw)

def process_method(data):
    data['bbb'] = data['bbb']/20
    return data

def process_raw_data(db, table, raw_db=None, raw_table=None, process=None):
    logging.info('processing data')
    if raw_table and (raw_db is None):
        raw_db = db
    elif raw_db and (raw_table is None):
        raw_table = table
    else:
        raise Exception("请提供原始数据库和表名")

    last, last_of_raw = compare_data(db, table, raw_db, raw_table)
    query = f'SELECT * FROM {raw_table} WHERE id>{last}'
    data_chunks = pd.read_sql(query, raw_db, chunksize=2000)

    for data in data_chunks:
        new_data = process(data)
        data_to_mysql(new_data, table, con=db)
    logging.info('data processed')
    return True


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 9),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2019, 1, 14),
}

dag = DAG(
    "sample_sql_process",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = PythonOperator(
    task_id="gen_data",
    python_callable=gen_raw_data,
    op_kwargs = {'n_rows': 30},
    dag=dag)

con = create_engine(
    'mysql://rootb:3x870OV649AMSn*@' +\
    '14.152.49.155:8998/zyr?charset=gbk').connect()

t2 = PythonOperator(
    task_id = 'process_data',
    python_callable = process_raw_data,
    op_kwargs = {'db':con, 'table':'test_db_2', 'raw_table':'test_db_1',
                 'process':process_method},
    dag=dag
)

t1 >> t2
