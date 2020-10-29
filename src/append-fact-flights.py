import json
import boto3
import csv
import uuid
from datetime import datetime
import pandas as pd
import re
import os
import logging
import psycopg2


POSTGRES = ''
DATABASE = ''
USER_NAME = ''
PASSWORD = ''
SCHEMA = ''
charset = ''
logging.basicConfig(level=logging.INFO)


def handle_fact_flights():
    """handle FACT_FLIGHTS table."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES,
            database=DATABASE,
            user=USER_NAME,
            password=PASSWORD
        )
        cur = conn.cursor()
        logging.info("SUCCESS: Connection to PostgreSQL instance succeeded")
        cur.execute("""insert into FACT_FLIGHTS
                      (AIR_ROUTE_SKEY,AIRPLANE_SKEY,TRANSACTIONID,DISTANCE,DISTANCEGROUP,DEPDELAY,DEPDELAYGT15,
                      NEXTDAYARR)
                       select A.AIR_ROUTE_SKEY,B.AIRPLANE_SKEY,C.TRANSACTIONID,C.DISTANCE,
                              case when C.DISTANCE<=100
                                   then '0-100 miles'
                                   else concat(cast((C.DISTANCE-1)/100*100+1 as varchar),'-',cast((C.DISTANCE-1)/100*100+100 as varchar),' miles')
                                   end as DISTANCEGROUP,
                              C.DEPDELAY,
                              case when C.DEPDELAY>15
                                   then 1
                                   else 0 
                                   end as DEPDELAYGT15,
                              case when (C.DEPTIME::numeric::integer> C.ARRTIME::numeric::integer) and 
                                        (C.DEPTIME::numeric::integer/100*60+ MOD(C.DEPTIME::numeric::integer/100*60,100)+
                                         C.ACTUALELAPSEDTIME::numeric::integer-
                                         (C.ARRTIME::numeric::integer/100*60+ MOD(C.ARRTIME::numeric::integer,100))>5*60)
                                   then 1
                                   else 0
                                   end as NEXTDAYARR
                       from  FLIGHTS_DLT C
                       left join DIM_AIR_ROUTE A
                       on  A.FLIGHTNUM=C.FLIGHTNUM and 
                           A.ORIGINAIRPORTCODE=C.ORIGINAIRPORTCODE and
                           A.DESTAIRPORTCODE=C.DESTAIRPORTCODE and
                           A.DISTANCE=C.DISTANCE 
                       left join DIM_AIRPLANE B
                       on B.TAILNUM=C.TAILNUM and
                          B.AIRLINECODE=C.AIRLINECODE and 
                          B.AIRLINENAME=C.AIRLINENAME
                     """)
        conn.commit()
        cur.close()
        logging.info("Append fact_table data successfully!")
    except Exception as e:
        logging.info(e)


handle_fact_flights()
