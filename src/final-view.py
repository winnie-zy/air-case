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


def handle_final_view():
    """creaate final view VW_FLIGHTS ."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES,
            database=DATABASE,
            user=USER_NAME,
            password=PASSWORD
        )
        cur = conn.cursor()
        logging.info("SUCCESS: Connection to PostgreSQL instance succeeded")
        cur.execute(""" create or replace view """ + SCHEMA + """.VW_FLIGHTS 
                            as
                            select  E.TRANSACTIONID,E.DISTANCEGROUP,E.DEPDELAYGT15,E.NEXTDAYARR,E.AIRLINENAME,
                            E.ORIGAIRPORTNAME,F.AIRPORTNAME as DESTAIRPORTNAME
                            from (
                            select A.TRANSACTIONID as TRANSACTIONID,
                                   A.DISTANCEGROUP as DISTANCEGROUP,
                                   A.DEPDELAYGT15 as DEPDELAYGT15,
                                   A.NEXTDAYARR as NEXTDAYARR,
                                   B.AIRLINENAME as AIRLINENAME,
                                   D.AIRPORTNAME as ORIGAIRPORTNAME,
                                   C.DESTAIRPORTCODE as DESTAIRPORTCODE
                                   from FACT_FLIGHTS as A
                                   inner join DIM_AIRPLANE as B
                                   on A.AIRPLANE_SKEY = B.AIRPLANE_SKEY
                                   inner join DIM_AIR_ROUTE as C
                                   on A.AIR_ROUTE_SKEY = C.AIR_ROUTE_SKEY
                                   inner join DIM_AIRPORT  as D
                                   on D.AIRPORTCODE=C.ORIGINAIRPORTCODE )E
                            inner join DIM_AIRPORT as F
                            on E.DESTAIRPORTCODE=F.AIRPORTCODE
                      """)
        conn.commit()
        cur.close()
        logging.info("Create view VW_FLIGHTS successfully!")

    except Exception as e:
        logging.info(e)


handle_final_view()
