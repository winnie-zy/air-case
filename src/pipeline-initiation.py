
import json
import csv
import uuid
from datetime import datetime
import pandas as pd
import re
import psycopg2
from io import StringIO
import logging
import os

LOCAL_FILE = ''
POSTGRES = ''
DATABASE = ''
USER_NAME = ''
PASSWORD = ''
SCHEMA = ''
charset = ''
logging.basicConfig(level=logging.INFO)


def start_init():
    """Preprocess the source data and create empty tables. 
    Loading the processed data into the transaction delta table."""
    processed_file = rawData_pre_process()
    create_postgre_tables()
    load_processed_file_to_flights_dlt(processed_file)


def create_postgre_tables():
    """Connect to the PostgreSQL database server and create empty tables including some staging 
       tables,DIM_*,FACT_FLIGHTS."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES,
            database=DATABASE,
            user=USER_NAME,
            password=PASSWORD
        )
        cur = conn.cursor()
        logging.info("Opened database successfully")
        # check if the schema contains some required tables
        cur.execute("""select exists(
                             select  1 from information_schema.tables 
                             where table_schema = '""" + SCHEMA + """' 
                             and table_name='flights_dlt'
 	         )""")
        table_num = cur.fetchone()
        if (table_num[0] is False):
            # create an empty staing table 'FLIGHTS_DLT' to store data after transformation
            cur.execute("""CREATE TABLE """ + SCHEMA + """.FLIGHTS_DLT(
                            TRANSACTIONID varchar(255),
                            FLIGHTDATE date,
                            AIRLINECODE varchar(255),
                            AIRLINENAME varchar(255),
                            TAILNUM varchar(255),
                            FLIGHTNUM varchar(255),
                            ORIGINAIRPORTCODE varchar(255),
                            ORIGAIRPORTNAME varchar(255),
                            ORIGINCITYNAME varchar(255),
                            ORIGINSTATE varchar(255),
                            ORIGINSTATENAME varchar(255),
                            DESTAIRPORTCODE varchar(255),
                            DESTAIRPORTNAME varchar(255),
                            DESTCITYNAME varchar(255),
                            DESTSTATE varchar(255),
                            DESTSTATENAME varchar(255),
                            CRSDEPTIME varchar(255),
                            DEPTIME varchar(255),
                            DEPDELAY integer,
                            TAXIOUT varchar(255),
                            WHEELSOFF varchar(255),
                            WHEELSON varchar(255),
                            TAXIIN varchar(255),
                            CRSARRTIME varchar(255),
                            ARRTIME varchar(255),
                            ARRDELAY varchar(255),
                            CRSELAPSEDTIME varchar(255),
                            ACTUALELAPSEDTIME varchar(255),
                            CANCELLED boolean,
                            DIVERTED boolean,
                            DISTANCE integer
                        )""")
            logging.info("SUCCESS: Create table  FLIGHTS_DLT succeessfully")
            # create an empty staging table 'STG_AIRPLANE_DLT' to store flights basic info from raw file
            cur.execute("""CREATE TABLE """ + SCHEMA + """.STG_AIRPLANE_DLT(
                               TAILNUM varchar(255),
                               AIRLINECODE varchar(255),
                               AIRLINENAME varchar(255)
                             )""")
            logging.info(
                "SUCCESS: Create table  STG_AIRPLANE_DLT succeessfully")
            # create an  empty staging table 'STG_AIRPORT_DLT' to store flight CRS info from raw file
            cur.execute("""CREATE TABLE """ + SCHEMA + """.STG_AIRPORT_DLT(
                               AIRPORTCODE varchar(255),
                               AIRPORTNAME varchar(255),
                               CITYNAME varchar(255)
                             )""")
            logging.info(
                "SUCCESS: Create table  STG_AIRPORT_DLT succeessfully")
            # create an empty staging table 'STG_AIR_ROUTE_DLT' to store air route info from raw file
            cur.execute("""CREATE TABLE """ + SCHEMA + """.STG_AIR_ROUTE_DLT(
                               FLIGHTNUM varchar(255),
                               ORIGINAIRPORTCODE varchar(255),
                               DESTAIRPORTCODE varchar(255),
                               DISTANCE integer
                             )""")
            logging.info(
                "SUCCESS: Create table  STG_AIR_ROUTE_DLT succeessfully")
            # create an empty dimention table 'DIM_AIRPLANE'
            cur.execute("""CREATE TABLE """ + SCHEMA + """.DIM_AIRPLANE(
                               AIRPLANE_SKEY SERIAL PRIMARY KEY,
                               TAILNUM varchar(255),
                               AIRLINECODE varchar(255),
                               AIRLINENAME varchar(255)
                             )""")
            logging.info("SUCCESS: Create table  DIM_AIRPLANE succeessfully")
            # create an  empty dimention table 'DIM_AIRPORT'
            cur.execute("""CREATE TABLE """ + SCHEMA + """.DIM_AIRPORT(
                               AIRPORT_SKEY SERIAL PRIMARY KEY,
                               AIRPORTCODE varchar(255),
                               AIRPORTNAME varchar(255),
                               CITYNAME varchar(255)
                             )""")
            logging.info("SUCCESS: Create table  DIM_AIRPORT succeessfully")
            # create an empty dimention table 'DIM_AIR_ROUTE'
            cur.execute("""CREATE TABLE """ + SCHEMA + """.DIM_AIR_ROUTE(
                               AIR_ROUTE_SKEY SERIAL PRIMARY KEY,
                               FLIGHTNUM varchar(255),
                               ORIGINAIRPORTCODE varchar(255),
                               DESTAIRPORTCODE varchar(255),
                               DISTANCE integer
                             )""")
            logging.info("SUCCESS: Create table  DIM_AIR_ROUTE succeessfully")
            # create an empty fact table ' FACT_FLIGHTS'
            cur.execute("""CREATE TABLE """ + SCHEMA + """. FACT_FLIGHTS(
                               AIR_ROUTE_SKEY integer,
                               AIRPLANE_SKEY integer,
                               TRANSACTIONID varchar(255),
                               DISTANCE integer,
                               DISTANCEGROUP varchar(255),
                               DEPDELAY integer,
                               DEPDELAYGT15 smallint,
                               NEXTDAYARR smallint
                             )""")
            logging.info("SUCCESS: Create table  FACT_FLIGHTS succeessfully")
        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        logging.error(e)
        logging.info(e)


def rawData_pre_process():
    """Preprocess the raw data"""
    try:
        df = pd.read_table(filepath_or_buffer=LOCAL_FILE, sep='|')
        logging.info(df.head(5))
        logging.info(df.shape)
        # format 'FLIGHTDATE'
        df['FLIGHTDATE'] = pd.to_datetime(df['FLIGHTDATE'], format='%Y%m%d')
        df['ACTUALELAPSEDTIME'] = df['ACTUALELAPSEDTIME'].astype(str)
        # drop records with 'CANCELLED','DIVERTED' which contain '1','T','True'value,as these flights info is meaningless.
        df = df.drop(df.loc[df['CANCELLED'] == 'True'].index)
        df = df.drop(df.loc[df['CANCELLED'] == 'T'].index)
        df = df.drop(df.loc[df['CANCELLED'] == '1'].index)
        df = df.drop(df.loc[df['DIVERTED'] == 'True'].index)
        df = df.drop(df.loc[df['DIVERTED'] == 'T'].index)
        df = df.drop(df.loc[df['DIVERTED'] == '1'].index)
        # drop records with 'TAILNUM' which is null, 'UNKNOW','#NAME?','NKNO' as it's meaningless for flights info analysis.
        df = df.drop(df.loc[df['TAILNUM'].isnull()].index)
        df = df.drop(df.loc[df['TAILNUM'] == 'NKNO'].index)
        df = df.drop(df.loc[df['TAILNUM'] == 'UNKNOW'].index)
        df['DEPDELAY'] = df['DEPDELAY'].astype(int)
        # Clean up the AIRLINENAME column by removing the airline code from it.
        df['AIRLINENAME'] = df['AIRLINENAME'].apply(lambda x: x.split(':')[0])
        # Clean up the ORIGAIRPORTNAME and DESTAIRPORTNAME columns by removing the concatenated city and state.
        df['ORIGAIRPORTNAME'] = df['ORIGAIRPORTNAME'].apply(
            lambda x: x.split(':')[1])
        # remove the space on the left of processed 'ORIGAIRPORTNAME'
        df['ORIGAIRPORTNAME'] = df['ORIGAIRPORTNAME'].apply(
            lambda x: x.lstrip())
        df['DESTAIRPORTNAME'] = df['DESTAIRPORTNAME'].apply(
            lambda x: x.split(':')[1])
        # remove the space on the left of processed 'DESTAIRPORTNAME'
        df['DESTAIRPORTNAME'] = df['DESTAIRPORTNAME'].apply(
            lambda x: x.lstrip())
        df['DISTANCE'] = df['DISTANCE'].apply(lambda x: x.split(' ')[0])
        logging.info(df.shape)
        logging.info(
            "SUCCESS: The raw file has been preprocessed sucessfully!")
        return df
    except Exception as e:
        logging.info(e)


def load_processed_file_to_flights_dlt(df):
    """Each load process, I truncate flights_dlt table and load the processed file and load the lastest info to 
       the staging table.Truncate flights_dlt is to ensure the information of flights_dlt is the 
       lastest before each load."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES,
            database=DATABASE,
            user=USER_NAME,
            password=PASSWORD
        )
        cur = conn.cursor()
        # truncate flights_dlt
        cur.execute("""truncate table flights_dlt """)
        logging.info("SUCCESS: Truncate table flights_dlt succeessfully!")
        conn.commit()
        # load the processed file and lastest info to the staging table
        output = StringIO()
        date = datetime.now().strftime("%H:%M:%S")
        logging.info(date)
        df.to_csv(output, sep='\t', index=False, header=False)
        output1 = output.getvalue()
        cur.copy_from(StringIO(output1), 'flights_dlt', columns=df.columns)
        conn.commit()
        cur.close()
        conn.close()
        date = datetime.now().strftime("%H:%M:%S")
        logging.info(date)
        logging.info("SUCCESS: Loading to table flights_dlt succeessfully!")
    except Exception as e:
        logging.info(e)


start_init()
