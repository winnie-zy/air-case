import json
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


def handle_dim_stg_air_route():
    """handle STG_AIR_ROUTE_DLT and DIM_AIR_ROUTE tables."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES,
            database=DATABASE,
            user=USER_NAME,
            password=PASSWORD
        )
        cur = conn.cursor()
        logging.info("SUCCESS: Connection to PostgreSQL instance succeeded")
        # truncate STG_AIR_ROUTE_DLT  to ensure the information of STG_AIR_ROUTE_DLT is the lastest.
        cur.execute("""truncate table STG_AIR_ROUTE_DLT """)
        logging.info("truncate table STG_AIR_ROUTE_DLT succeessfully!")
        # initiate STG_AIR_ROUTE_DLT,deduplication
        cur.execute("""insert into STG_AIR_ROUTE_DLT 
                           select FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,CRSDEPTIME,CRSARRTIME,DISTANCE
                           from FLIGHTS_DLT 
                           group by FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,CRSDEPTIME,CRSARRTIME,DISTANCE""")
        conn.commit()
        logging.info("Initiate STG_AIR_ROUTE_DLT successfully!")
        # upsert DIM_AIR_ROUTE
         # step 1 Find the brand new dim(insert new and updated DIM_AIR_ROUTE records)
        sql="""
              insert into DIM_AIR_ROUTE
              (FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,CRSDEPTIME,CRSARRTIME,DISTANCE,
              CURRENT_FLAG,EFFT_DATE,EXPY_DATE)
              select DLT.FLIGHTNUM,DLT.ORIGINAIRPORTCODE,DLT.DESTAIRPORTCODE,DLT.CRSDEPTIME,DLT.CRSARRTIME,
                     DLT.DISTANCE,'Y' as CURRENT_FLAG,current_date as EFFT_DATE,
	                 to_date('9999-12-31','YYYY-MM-DD') as EXPY_DATE
              from STG_AIR_ROUTE_DLT DLT
              left join (
	                      select 
	                      *
	                      from DIM_AIR_ROUTE
	                      where current_date between EFFT_DATE and  EXPY_DATE 
                        )DIM
              on DLT.FLIGHTNUM=DIM.FLIGHTNUM  and 
                 DLT.ORIGINAIRPORTCODE=DIM.ORIGINAIRPORTCODE and 
                 DLT.DESTAIRPORTCODE=DIM.DESTAIRPORTCODE and 
                 DLT.DISTANCE=DIM.DISTANCE 
              where DIM.FLIGHTNUM is null or
                    DIM.ORIGINAIRPORTCODE is null or
	                DIM.DESTAIRPORTCODE is null or
	                DIM.DISTANCE is null
              union
              select DLT.FLIGHTNUM,DLT.ORIGINAIRPORTCODE,DLT.DESTAIRPORTCODE,DLT.CRSDEPTIME,DLT.CRSARRTIME,
                     DLT.DISTANCE,'Y' as CURRENT_FLAG,current_date as EFFT_DATE,
	                 to_date('9999-31-12','YYYY-MM-DD') as EXPY_DATE
              from (
	                 select FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,CRSDEPTIME,CRSARRTIME,DISTANCE,
	                        MD5(CONCAT('_',CRSDEPTIME,CRSARRTIME)) as content
	                 from STG_AIR_ROUTE_DLT DLT
                   )DLT
              inner join(
	                 select FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,DISTANCE,
	                        MD5(CONCAT('_',CRSDEPTIME,CRSARRTIME)) as content
	                 from DIM_AIR_ROUTE
	                 where current_date between EFFT_DATE and  EXPY_DATE 
	                    ) DIM
              on DLT.FLIGHTNUM=DIM.FLIGHTNUM and
                 DLT.ORIGINAIRPORTCODE=DIM.ORIGINAIRPORTCODE and
                 DLT.DESTAIRPORTCODE=DIM.DESTAIRPORTCODE and
                 DLT.DISTANCE=DIM.DISTANCE
              where DLT.content!=DIM.content
        """
        cur.execute(sql)
        conn.commit()
         #step 2 expire old record
        expire_sql="""
                       update DIM_AIR_ROUTE base
                       set EXPY_DATE=current_date -1,CURRENT_FLAG='N' 
                       from(
	                        select *
	                        from(
	                              select DIM.FLIGHTNUM,DIM.ORIGINAIRPORTCODE,
                                         DIM.DESTAIRPORTCODE,DIM.DISTANCE,DIM.EFFT_DATE,
                                         rank() over(partition by DIM.FLIGHTNUM,
                                         DIM.ORIGINAIRPORTCODE,DIM.DESTAIRPORTCODE,
                                         DIM.DISTANCE order by DIM.EFFT_DATE) as RANK_EFFT
	                              from(
	                                    select FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,
                                               DISTANCE,EFFT_DATE,
                                               count(*) over(partition by FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,
								               DISTANCE) as CNT
	                                    from DIM_AIR_ROUTE
	                                    where current_date between EFFT_DATE and  EXPY_DATE 
	                                  )DIM
	                              where DIM.CNT>1 
		                        )A
	                        where A.RANK_EFFT=1 
                            )exp
                       where base.EFFT_DATE=exp.EFFT_DATE and
                             base.FLIGHTNUM=exp.FLIGHTNUM and 
                             base.ORIGINAIRPORTCODE=exp.ORIGINAIRPORTCODE and
                             base.DESTAIRPORTCODE=exp.DESTAIRPORTCODE and
                             base.DISTANCE=exp.DISTANCE
                    """
        cur.execute(expire_sql)
        conn.commit()
        cur.close()
        logging.info("Upsert  DIM_AIR_ROUTE successfully!")

    except Exception as e:
        logging.info(e)


handle_dim_stg_air_route()
