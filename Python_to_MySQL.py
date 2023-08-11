import mysql.connector
import argparse
import pandas as pd
import numpy as np
from mysql.connector import Error
import os
import json
from datetime import datetime
import re


def convert_sr_no(sr_no):
    if pd.isna(sr_no):
        sr_no = 0
    else:
        sr_no = int(sr_no)
    return sr_no

def required_date(tempdate):
    try:
        new_date = tempdate[6:] + "/" + tempdate[3:5] + "/" + tempdate[0:2]
        return datetime.strptime(new_date, "%Y/%m/%d").date()
    except ValueError:
        try:
            return datetime.strptime(tempdate, "%d/%m/%Y").date()
        except ValueError:
            # Handle unknown date format or invalid date
            return None

def Amount(amount):
    if isinstance(amount, str):
        amount = amount.replace(',', '')

        if amount == 'N/A' or amount == '':
            amount = '0'

    elif pd.isna(amount):
        amount = '0'

    try:
        return int(amount)
    except ValueError:
        # Handle invalid amount value
        return None


def clean_value(value):
    if pd.isna(value):
        cleaned_value = ''
    else:
        cleaned_value = str(value)
    cleaned_value = re.sub(r'\\x[0-9a-zA-Z]{2}', ' ', cleaned_value)
    cleaned_value = re.sub(r'[^\x00-\x7F]', ' ', cleaned_value)      # Remove non-ASCII characters
    return cleaned_value


    
def create_startup_table(cursor,table_name):
    try:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Sr_No INT,
            Date DATE,
            Startup_Name VARCHAR(255),
            Industry_Vertical VARCHAR(255),
            SubVertical VARCHAR(255),
            City VARCHAR(255),
            Investors_Name VARCHAR(255),
            InvestmentnType VARCHAR(255),
            Amount_in_USD INT
        )
        """
        cursor.execute(create_table_query)
        print(f"Table {table_name} created successfully.")
    except Error as e:
        print("Error creating table:", e)


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--source_dir', help="Source directory from which we need to read all files to upload to mysql", type=str)
parser.add_argument('-c', '--mysql_details', help="Path of a JSON file which contains all details of MySQL to which to connect to", type=str)
parser.add_argument('-t', '--destination_table', help='Name of table to upload this data to', type=str)

args = parser.parse_args()

df1 = pd.read_csv(os.path.join(args.source_dir, "startup.csv"))
df2 = pd.read_parquet(os.path.join(args.source_dir, "consumerInternet.parquet"))

final_df = pd.concat([df1, df2])

# print(final_df)

with open(args.mysql_details, 'r') as f:
    mysql_detail = json.load(f)

Connection = mysql.connector.connect(
    host=mysql_detail['host'],
    user=mysql_detail['username'],
    password=mysql_detail['password'],
    database=mysql_detail['database']
)

if Connection.is_connected():
    print("MySQL is connected")


try:
    table_name = args.destination_table
    create_startup_table(Connection.cursor(), table_name)

    for index, row in final_df.iterrows():
        Sr_no = convert_sr_no(row['Sr_No'])
        Date = required_date(row['Date'])
        Startup_Name = clean_value(row['Startup_Name'])
        Industry_Vertical = clean_value(row['Industry_Vertical'])
        SubVertical = clean_value(row['SubVertical'])
        City = clean_value(row['City'])
        Investors_Name = clean_value(row['Investors_Name'])
        InvestmentnType = clean_value(row['InvestmentnType'])
        Amount_in_USD = Amount(row['Amount_in_USD'])

    


        try:
            cursor = Connection.cursor()
            insert_query = f"INSERT INTO {args.destination_table} (Sr_No, Date, Startup_Name, Industry_Vertical, SubVertical, City, Investors_Name, InvestmentnType, Amount_in_USD) " \
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_query, (Sr_no, Date, Startup_Name, Industry_Vertical, SubVertical, City, Investors_Name, InvestmentnType, Amount_in_USD))
            Connection.commit()
            cursor.close()

        except Error as e:
            if e.errno == 1146:  # Check if the error is "Table not found" error
                print("Table 'startup' not found. Creating the table...")
                create_startup_table(Connection.cursor())
                print("Table created. Inserting data into the table...")
                # Retry the data insertion after creating the table
                cursor = Connection.cursor()
                cursor.execute(insert_query, (Sr_no, Date, Startup_Name, Industry_Vertical, SubVertical, City, Investors_Name, InvestmentnType, Amount_in_USD))
                Connection.commit()
                cursor.close()
            else:
                print("Error inserting row:", index)
                print(e)

except Error as e:
    print("Error connecting to MySQL:", e)

Connection.close()
