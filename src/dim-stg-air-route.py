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
charset = ''
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
                           select FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,DISTANCE
                           from FLIGHTS_DLT 
                           group by FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,DISTANCE""")
        conn.commit()
        logging.info("Initiate STG_AIR_ROUTE_DLT successfully!")
        # upsert DIM_AIR_ROUTE
        cur.execute("""insert into DIM_AIR_ROUTE
                           (FLIGHTNUM,ORIGINAIRPORTCODE,DESTAIRPORTCODE,DISTANCE) 
                           select a_dlt.FLIGHTNUM,a_dlt.ORIGINAIRPORTCODE,a_dlt.DESTAIRPORTCODE,
                           a_dlt.DISTANCE
                           from STG_AIR_ROUTE_DLT as a_dlt
                           left join DIM_AIR_ROUTE as a
                           on a_dlt.FLIGHTNUM=a.FLIGHTNUM  and 
                           a_dlt.ORIGINAIRPORTCODE=a.ORIGINAIRPORTCODE and 
                           a_dlt.DESTAIRPORTCODE=a.DESTAIRPORTCODE and 
                           a_dlt.DISTANCE=a.DISTANCE 
                           where a.FLIGHTNUM is null or
                                 a.ORIGINAIRPORTCODE is null or
                                 a.DESTAIRPORTCODE is null or
                                 a.DISTANCE is null """)
        conn.commit()
        cur.close()
        logging.info("Upsert  DIM_AIR_ROUTE successfully!")

    except Exception as e:
        logging.info(e)


handle_dim_stg_air_route()
