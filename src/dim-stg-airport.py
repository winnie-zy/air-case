import json
import boto3
import csv
import uuid
from datetime import datetime
import pandas as pd
import re
import logging
import os
import psycopg2

POSTGRES = ''
DATABASE = ''
USER_NAME = ''
PASSWORD = ''
SCHEMA = ''
logging.basicConfig(level=logging.INFO)


def handle_dim_stg_airport():
    """handle STG_AIRPORT_DLT and DIM_AIRPORT tables."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES,
            database=DATABASE,
            user=USER_NAME,
            password=PASSWORD
        )
        cur = conn.cursor()
        logging.info("SUCCESS: Connection to PostgreSQL instance succeeded")
        # truncate STG_AIRPORT_DLT  to ensure the information of STG_AIRPORT_DLT is the lastest.
        cur.execute("""truncate table STG_AIRPORT_DLT """)
        logging.info("truncate table STG_AIRPORT_DLT succeessfully!")
        # initiate STG_AIRPORT_DLT,deduplication
        cur.execute("""insert into STG_AIRPORT_DLT 
                           select ORIGINAIRPORTCODE,ORIGAIRPORTNAME,ORIGINCITYNAME
                           from FLIGHTS_DLT 
                           group by ORIGINAIRPORTCODE,ORIGAIRPORTNAME,ORIGINCITYNAME
                           union
						   select DESTAIRPORTCODE,DESTAIRPORTNAME,DESTCITYNAME
                           from FLIGHTS_DLT 
                           group by DESTAIRPORTCODE,DESTAIRPORTNAME,DESTCITYNAME
                           """)
        conn.commit()
        logging.info("Initiate STG_AIRPORT_DLT successfully!")
        # upsert _AIRPORT
        # step 1 Find the brand new dim(insert new and updated DIM_AIRPORT records)
        sql="""
        insert into DIM_AIRPORT
        (AIRPORTCODE,AIRPORTNAME,CITYNAME,CURRENT_FLAG,EFFT_DATE,EXPY_DATE)
        select DLT.AIRPORTCODE,DLT.AIRPORTNAME,DLT.CITYNAME,'Y' as CURRENT_FLAG,
               current_date as EFFT_DATE,to_date('9999-12-31','YYYY-MM-DD') as EXPY_DATE
        from STG_AIRPORT_DLT  DLT
        left join 
        (select *
	     from DIM_AIRPORT
         where current_date between EFFT_DATE and EXPY_DATE
        )  DIM
        on DLT.AIRPORTCODE=DIM.AIRPORTCODE
        where DIM.AIRPORTCODE is null 
        union
        select DLT.AIRPORTCODE,DLT.AIRPORTNAME,DLT.CITYNAME,'Y' as CURRENT_FLAG,
                  current_date as EFFT_DATE,to_date('9999-12-31','YYYY-MM-DD') as EXPY_DATE
        from(
               select AIRPORTCODE,AIRPORTNAME,CITYNAME,
               MD5(CONCAT('_',AIRPORTNAME,CITYNAME)) as content
               from STG_AIRPORT_DLT  )DLT
        inner join(
	           select AIRPORTCODE,AIRPORTNAME,CITYNAME,
                      MD5(CONCAT('_',AIRPORTNAME,CITYNAME)) as content
               from DIM_AIRPORT  
               where current_date between EFFT_DATE and EXPY_DATE )DIM
        on DLT.AIRPORTCODE=DIM.AIRPORTCODE  
        where DLT.content!=DIM.content
        """
        cur.execute(sql)
        conn.commit()
        #step 2 expire old record
        expire_sql="""
        update DIM_AIRPORT base
        set EXPY_DATE=current_date,CURRENT_FLAG='N'
        from(
           select *
	       from(
	          select DIM.AIRPORTCODE,DIM.AIRPORTNAME,DIM.CITYNAME,DIM.EFFT_DATE,
	                 rank() over(partition by DIM.AIRPORTCODE order by DIM.EFFT_DATE) as EFFT_RANK
	          from(
	               select AIRPORTCODE,AIRPORTNAME,CITYNAME,EFFT_DATE,
	                      count(*) over(partition by AIRPORTCODE) as CNT
	               from DIM_AIRPORT
	               where current_date between EFFT_DATE and EXPY_DATE
	              )DIM
	          where DIM.CNT>1
	           )A
	       where A.EFFT_RANK=1
           )EXP
        where base.AIRPORTCODE=EXP.AIRPORTCODE and base.EFFT_DATE=EXP.EFFT_DATE
        """
        cur.execute(expire_sql)
        conn.commit()
        cur.close()
        logging.info("Upsert  DIM_AIRPORT successfully!")

    except Exception as e:
        logging.info(e)


handle_dim_stg_airport()
