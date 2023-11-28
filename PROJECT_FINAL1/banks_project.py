# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup as soup 
import pandas as pd
import sqlite3 
from datetime import datetime
import numpy as np


url="https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_intial=["Name","MC_USD_Billion"]
table_attribs=["Name","MC_USD_Billion","MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
csv_path="./Largest_banks_data.csv"
csv_file="exchange_rate.csv"
db_name="Banks.db"
table_name="Largest_banks"
log_file="code_log.txt"


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 


def extract(url,table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    df= pd.DataFrame(columns=table_attribs)
    html_page=requests.get(url).text
    data= soup(html_page,'html.parser')
    tables=data.find_all('tbody')
    rows=tables[0].find_all('tr')
    for row in rows:
        col=row.find_all('td')
        if (len(col)!=0 ):
            Name=(col[1].find_all('a')[1]['title']).replace('\n','')
            MC_USD_Billion=float((col[2].contents[0]).replace('\n',''))
            df1= pd.DataFrame({"Name":Name,"MC_USD_Billion":MC_USD_Billion},index=[0])
            df=pd.concat([df,df1], ignore_index=True)

    return df
   

def transform(df,csv_file):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    dataframe= pd.read_csv(csv_file)
    exchange_rate = dataframe.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)


def run_query(query_statement, sql_connection):
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_statement)
    print(query_output)


log_progress("Preliminaries complete. Initiating ETL process")
extracted_data=extract(url,table_intial)
log_progress("Data extraction complete. Initiating Transformation process")
transformed_data=transform(extracted_data,csv_file)
log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(transformed_data,csv_path)
log_progress("Data saved to CSV file")
sql_connection=sqlite3.connect(db_name)
log_progress("SQL Connection initiated")
load_to_db(transformed_data, sql_connection, table_name)
log_progress("Data loaded to Database as a table, Executing queries")
query_statement=f"SELECT * FROM Largest_banks"
run_query(query_statement,sql_connection )
query_statement=f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement,sql_connection )
query_statement=f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement,sql_connection )
log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")