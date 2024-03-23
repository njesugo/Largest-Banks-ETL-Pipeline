from datetime import datetime 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np 

log_file="code_log_process.txt"

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    current_time = datetime.now()
    timestamp = current_time.strftime(timestamp_format)
    with open(log_file, "a") as file:
        file.write(timestamp + ' : ' + message + '\n')

log_progress("Declaring known values")

url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_atributes_init=["Name", "MC_USD_Billion"]
table_atributes_final=["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
target_csv_file = "./largest_banks_data.csv"
database = "Banks.db"
table = "Largest_banks"


log_progress("Preliminaries complete. Initiating ETL process")


def extract(url, table_atributes_init):
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')
    tables = soup.find_all("tbody")
    rows = tables[0].find_all("tr")
    dataframe = pd.DataFrame(columns=table_atributes_init)
    for row in rows:
        columns = row.find_all('td')
        if(len(columns)):
            a_links = columns[1].find_all('a')
            if(a_links is not None):
                data_dict = {
                    "Name": a_links[1]['title'],
                    "MC_USD_Billion": float(columns[2].contents[0][:-1])
                }
                dataframe_init = pd.DataFrame(data_dict, index=[0])
                dataframe = pd.concat([dataframe, dataframe_init], ignore_index=True)

    return dataframe

log_progress("Call extract() function")
extracted_data = extract(url, table_atributes_init)

# print the contents of the returning extracted data frame
print(extracted_data)
log_progress("Data extraction complete. Initiating Transformation process")

def transform(dataframe):
    exchange_rate = pd.read_csv("./exchange_rate.csv")
    dataframe_dict = exchange_rate.set_index("Currency").to_dict()["Rate"]
    dataframe["MC_EUR_Billion"] = [np.round(x*dataframe_dict["EUR"],2) for x in dataframe["MC_USD_Billion"]]
    dataframe["MC_GBP_Billion"] = [np.round(x*dataframe_dict["GBP"],3) for x in dataframe["MC_USD_Billion"]]
    dataframe["MC_INR_Billion"] = [np.round(x*dataframe_dict["INR"],2) for x in dataframe["MC_USD_Billion"]]
    return dataframe

log_progress("Call transform() function")

# print the contents of the returning transformed data frame
transformed_data = transform(extracted_data)
print(transformed_data)
print(transformed_data['MC_EUR_Billion'][4])
log_progress("Data transformation complete. Initiating Loading process")

def load_to_csv(transformed_data, target_csv_file):
    transformed_data.to_csv(target_csv_file)

def load_to_db(transformed_data, table, connection):
    transformed_data.to_sql(table, connection, if_exists='replace', index=False)


def run_queries(query, connection):
    query_output = pd.read_sql(query, connection)
    print(query_output)

log_progress("Call load_to_csv()")
load_to_csv(transformed_data, target_csv_file)
log_progress("Data saved to CSV file")

log_progress("Initiate SQLite3 connection")
connection = sqlite3.connect(database)
log_progress("SQL Connection initiated")


log_progress("Call load_to_db()")
load_to_db(transformed_data, table, connection)
log_progress("Data loaded to Database as a table, Executing queries")

request_1= f"SELECT * FROM Largest_banks"
request_2= f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
request_3= f"SELECT Name from Largest_banks LIMIT 5"

log_progress("Call run_query()")
run_queries(request_1, connection)
run_queries(request_2, connection)
run_queries(request_3, connection)

log_progress("Process Complete")

log_progress("Close SQLite3 connection")
connection.close()
log_progress("Server Connection closed")

print(transformed_data['MC_EUR_Billion'][4])

#146.86
#151.987