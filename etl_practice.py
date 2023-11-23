import pandas as pd
import glob
import xml.etree.ElementTree as ET
from datetime import datetime 


log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

def extract_csv(file_process):
    dataframe=pd.read_csv(file_process)
    return dataframe 

def extract_json(file_process):
    dataframe=pd.read_json(file_process, lines=True)
    return dataframe


def extract_xml(file_process):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price","fuel"]) 
    tree = ET.parse(file_process)
    root = tree.getroot()
    for element in root:
        car_model= element.find('car_model').text
        year_of_manufacture = float(element.find("year_of_manufacture").text)
        price= float(element.find("price").text)
        fuel=element.find('fuel').text
        dataframe=pd.concat([dataframe, pd.DataFrame([{"car_model":car_model, "year_of_manufacture":year_of_manufacture, "price":price,"fuel":fuel}])] )
    return dataframe


def extract():
    extracted_file = pd.DataFrame()

    for file in glob.glob("*.csv"):
        dataframe=extract_csv(file)
        extracted_file=pd.concat([extracted_file,dataframe],ignore_index=True)
    
    for file in glob.glob("*.json"):
        dataframe=extract_json(file)
        extracted_file=pd.concat([extracted_file,dataframe],ignore_index=True)
    
    for file in glob.glob("*.xml"):
        dataframe=extract_xml(file)
        extracted_file=pd.concat([extracted_file,dataframe],ignore_index=True)

    return extracted_file

def transform(data):   
    data['price'] = round (data['price'],2)
    return data 


def load (target_file,data):
    transform(data).to_csv(target_file )
    


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 


log_progress("ETL Job Started") 



log_progress("Extract phase Started") 
extracted_data = extract()
print(extracted_data)
log_progress("Extract phase Ended") 

log_progress("Transform phase Started") 
transformed_data=transform(extracted_data)
print("transformed data ")
print(transformed_data)
log_progress("Transform phase Ended") 


log_progress("Load phase Started") 
load (target_file,transformed_data)
log_progress("Load phase Ended")



log_progress("ETL Job Ended")


