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
charset = ''
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
        cur.execute("""insert into DIM_AIRPLANE(TAILNUM,AIRLINECODE,AIRLINENAME) 
                           select a_dlt.TAILNUM,a_dlt.AIRLINECODE,a_dlt.AIRLINENAME
                           from STG_AIRPLANE_DLT as a_dlt
                           left join DIM_AIRPLANE as a
                           on a_dlt.TAILNUM=a.TAILNUM and 
                              a_dlt.AIRLINECODE=a.AIRLINECODE and
                              a_dlt.AIRLINENAME=a.AIRLINENAME
                           where a.TAILNUM is null or 
                                 a.AIRLINECODE is null or
                                 a.AIRLINENAME is null
                                 """)
        conn.commit()
        cur.close()
        logging.info("Upsert DIM_AIRPLANE successfully!")

    except Exception as e:
        logging.info(e)


handle_dim_stg_airplane()
