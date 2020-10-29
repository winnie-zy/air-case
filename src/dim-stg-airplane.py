import json
import boto3
import csv
import uuid
from datetime import datetime
import pandas as pd
import re
import psycopg2
import logging
import os

POSTGRES = ''
DATABASE = ''
USER_NAME = ''
PASSWORD = ''
SCHEMA = ''
logging.basicConfig(level=logging.INFO)


def handle_dim_stg_airplane():
    """handle STG_AIRPLANE_DLT and DIM_AIRPLANE tables."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES,
            database=DATABASE,
            user=USER_NAME,
            password=PASSWORD
        )
        cur = conn.cursor()
        logging.info("SUCCESS: Connection to PostgreSQL instance succeeded")
        # truncate STG_AIRPLANE_DLT  to ensure the information of STG_AIRPLANE_DLT is the lastest.
        cur.execute("""truncate table STG_AIRPLANE_DLT """)
        logging.info("truncate table STG_AIRPLANE_DLT succeessfully!")
        # initiate STG_AIRPLANE_DLT,deduplication
        cur.execute("""insert into STG_AIRPLANE_DLT 
                           select TAILNUM,AIRLINECODE,AIRLINENAME
                           from FLIGHTS_DLT 
                           group by TAILNUM,AIRLINECODE,AIRLINENAME
                           """)
        conn.commit()
        logging.info("Initiate STG_AIRPLANE_DLT successfully!")
        # upsert DIM_AIRPLANE
        #step 1 Find the brand new dim(insert new and updated DIM_AIRPLANE records)
        sql="""insert into DIM_AIRPLANE 
              (TAILNUM,AIRLINECODE,AIRLINENAME,CURRENT_FLAG,EFFT_DATE, EXPY_DATE)
              select DLT.TAILNUM,DLT.AIRLINECODE,DLT.AIRLINENAME,
                    'Y' as CURRENT_FLAG,current_date as EFFT_DATE,
                    to_date('9999-12-31','YYYY-MM-DD') as EXPY_DATE
              from STG_AIRPLANE_DLT DLT
              left join 
              (select  * 
               from DIM_AIRPLANE 
               where current_date between EFFT_DATE and EXPY_DATE
              ) DIM
              on DIM.TAILNUM=DLT.TAILNUM
              where DIM.TAILNUM is null
              union
              select DLT.TAILNUM,DLT.AIRLINECODE,DLT.AIRLINENAME,
                         Y' as CURRENT_FLAG,current_date as EFFT_DATE,
                         to_date('9999-12-31','YYYY-MM-DD') as EXPY_DATE
               from(
	                  select  TAILNUM,AIRLINECODE,AIRLINENAME,
                              MD5(CONCAT_WS('-',AIRLINECODE,AIRLINENAME)) as content
                      from STG_AIRPLANE_DLT )DLT
               inner join (
	                      select TAILNUM,MD5(CONCAT_WS('-',AIRLINECODE,AIRLINENAME)) as content
	                      from DIM_AIRPLANE
	                      where current_date between EFFT_DATE and EXPY_DATE
                                 )DIM
               on DLT.TAILNUM=DIM.TAILNUM 
               where DLT.content!=DIM.content"""
        cur.execute(sql)
        conn.commit()        
        #expire old record
        expire_sql="""update DIM_AIRPLANE base
                      set EXPY_DATE=current_date-1,CURRENT_FLAG='N'
                      from(
                        select A.TAILNUM,A.EFFT_DATE 
                        from(
	                      select DIM.TAILNUM,DIM.EFFT_DATE,
                                 rank() over (partition by DIM.TAILNUM order by DIM.EFFT_DATE )as EFFT_RANK
	                      from(
	                          select TAILNUM,EFFT_DATE,
                                     count(1) over(partition by TAILNUM) as CNT
	                          from DIM_AIRPLANE
	                          where current_date between EFFT_DATE and EXPY_DATE
                              )as DIM
	                      where DIM.CNT>1) as A
                        where A.EFFT_RANK=1
                          )exp
                      where base.TAILNUM=exp.TAILNUM and base.EFFT_DATE = exp.EFFT_DATE"""
        cur.execute(expire_sql)
        conn.commit()
        cur.close()
        logging.info("Upsert DIM_AIRPLANE successfully!")

    except Exception as e:
        logging.info(e)


handle_dim_stg_airplane()
