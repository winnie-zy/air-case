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
charset = ''
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
                           """)
        conn.commit()
        logging.info("Initiate STG_AIRPORT_DLT successfully!")
        # upsert _AIRPORT
        cur.execute("""insert into DIM_AIRPORT(AIRPORTCODE,AIRPORTNAME,CITYNAME) 
                           select a_dlt.AIRPORTCODE,a_dlt.AIRPORTNAME,a_dlt.CITYNAME
                           from STG_AIRPORT_DLT as a_dlt
                           left join DIM_AIRPORT as a
                           on a_dlt.AIRPORTCODE=a.AIRPORTCODE and 
                              a_dlt.AIRPORTNAME=a.AIRPORTNAME and
                              a_dlt.CITYNAME=a.CITYNAME
                           where a.AIRPORTCODE is null or 
                                 a.AIRPORTNAME is null or 
                                 a.CITYNAME is null""")
        conn.commit()
        cur.close()
        logging.info("Upsert  DIM_AIRPORT successfully!")

    except Exception as e:
        logging.info(e)


handle_dim_stg_airport()
