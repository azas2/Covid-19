from io import StringIO
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import requests
import os
import pandas as pd
import time
import glob
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import shutil

def get_files_from_github():
    
    all_date = []
    api_url = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports"
    response = requests.get(api_url)
    save_folder="/tmp/covid_19_data"

    if os.path.exists(save_folder):
            # check if the folder is empty
                if not os.listdir(save_folder):
                    all_date.append(datetime.strptime('01-01-1990', '%m-%d-%Y').date())
                else:
                    for file in os.listdir(save_folder):
                        file_date = file.split('.')[0]
                        date_object = datetime.strptime(file_date, '%m-%d-%Y').date()
                        all_date.append(date_object)
    else:
        os.makedirs(save_folder)
        all_date.append(datetime.strptime('01-01-1990', '%m-%d-%Y').date())

    try:
        if response.status_code == 200:
            files=response.json()
            for file in files:
                if file['type']=='file' and file['name'].endswith('.csv'):
                    file_name=file['name']
                    download_url=file['download_url']
                    print(f'start to Downloading {file_name}')
                    try:
                        file_response=requests.get(download_url)
                        if file_response.status_code==200:
                            print(f'last load:{max(all_date)}')
                            if  max(all_date)<datetime.strptime(file_name.split('.')[0], '%m-%d-%Y').date():
                                with open(f'{save_folder}/{file_name}','wb') as f:
                                    f.write(file_response.content)
                                    print(f"Saved {file_name} to {save_folder}")
                                    time.sleep(1)
                            else:
                                logging.info("No new data to download")
                        else:
                            logging.error(f'Failed to download {file_name}')
                    except Exception as e:
                        
                        logging.error(f'ErrorInDownloder: {e}')
    except Exception as e:
        
        logging.error(f"Error while merging files: {e}")
    except requests.exceptions.RequestException as e:
       
        logging.error(f"HTTP Request failed: {e}")


def merge_all_files():
    try:
        combined_file = "/tmp/combined_data.csv"
        if os.path.exists(combined_file):
            os.remove(combined_file)
        
        files = glob.glob('/tmp/covid_19_data/*.csv')
        if not files:
            logging.info('No files found to merge')
            return
        logging.info(f'Found {len(files)} files to merge: {files}')
        
        dfs=[]
        for file in files:
         df = pd.read_csv(file,encoding='utf-8')
         if df.empty:
             logging.warning(f'enmpty File:{file}')
        else:
            logging.info(f'Read {file} with shape {df.shape}')
            dfs.append(df)
        if not dfs:
            logging.info('No data to merge')
            return 
        df = pd.concat(dfs, ignore_index=True)
        logging.info(f'Concatenated DataFrame shape: {df.shape}')
        logging.info(f'Columns in final DataFrame: {df.columns}')
        
        
        df['Last_Update'] = pd.to_datetime(df['Last_Update']).dt.date
        df['Country_Region'] = df['Country_Region'].str.replace('US', 'United States')
        df = df.sort_values(by='Last_Update')
        df.rename(columns={'Last_Update': 'Date'}, inplace=True)
          
        logging.info(f'Final DataFrame shape: {df.shape}')
        logging.info(f'First few rows:\n{df.head()}')
        
        df.to_csv(combined_file, index=False)
        logging.info("Files merged and saved successfully")
    except Exception as e:
        logging.error(f"Error in merge: {e}")
        
        
def _get_date_dim():
    output_data_file = "/tmp/data_dim.csv"
    combined_file = "/tmp/combined_data.csv"
    try:
        if not os.path.exists(combined_file):
            logging.error(f"File {combined_file} not found.")
            return
        df = pd.read_csv(combined_file)
        if 'Date' not in df.columns:
            raise ValueError("The 'Date' column is missing from combined_data.csv")
        
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date'])
        
        new_df = pd.DataFrame({
            'date_pk': df['Date'].dt.strftime('%Y%m%d').astype(str),
            'Date': df['Date'].dt.date,
            'Day': df['Date'].dt.day_name().str[:3],
            'Month': df['Date'].dt.month_name().str[:3],
            'Quarter': df['Date'].dt.quarter.astype(int),
            'Year': df['Date'].dt.year.astype(int)
        })
        new_df = new_df.drop_duplicates().sort_values(by='Date')
        print(new_df)
        new_df.to_csv(output_data_file, index=False, header=True)
        logging.info('data_file is saved in csv')
    except Exception as e:
        logging.error(f'Error in get_date_dim: {e}')
        
def _get_location_dim():
    try:
        file_data = '/tmp/combined_data.csv'
        combined_file = "/tmp/location_dim.csv"
        if os.path.exists(combined_file):
            os.remove(combined_file)
        df = pd.read_csv(file_data)
        new_df = pd.DataFrame({'RegionName':df['Country_Region'].astype(str)})
        new_df = new_df.drop_duplicates()
        new_df['RegionName'] = new_df['RegionName'].fillna('Unknown')
        new_df.to_csv('/tmp/location_dim.csv', index=False, columns=['RegionName'])
        print(50*'#','this is loaction data',50*'#')
        print(new_df)
    except Exception as e:
        print(f"in location {e}")
   
        
def _store_date_dim():
    DATE_DIM_CSV = '/tmp/data_dim.csv'
    POSTGRES_CONN_ID = 'postgres'
    SOURCE_TABLE = 'date_dim_3'
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        logging.info('startimg to store date_dim')
        
        date_dim_df = pd.read_csv(DATE_DIM_CSV)
        date_dim_df['Date'] = pd.to_datetime(date_dim_df['Date'])
        date_dim_df.columns = ['datepk', 'Date', 'day', 'month', 'quarter', 'year']

        check_empty_query = f"SELECT COUNT(*) FROM {SOURCE_TABLE}"
        count=hook.get_first(check_empty_query)[0]
            
        if count==0:
            with open(DATE_DIM_CSV, 'r', encoding='utf-8') as f:
                next(f)  # Skip header
                conn = hook.get_conn()
                cursor = conn.cursor()
                cursor.copy_from(f, SOURCE_TABLE, sep=',')
                conn.commit()
                cursor.close()
            logging.info(f"Inserted all data into {SOURCE_TABLE}.")
        else:      
                # Get the max date from the date_dim_1 table
            max_date_query = f"SELECT MAX(date) FROM {SOURCE_TABLE}"
            max_date_result = hook.get_first(max_date_query)
            max_date = pd.to_datetime(max_date_result[0])  if max_date_result else None
        if max_date:
                 new_data = date_dim_df[date_dim_df['Date'] > max_date]
                 if not new_data.empty:
                    output = StringIO()
                    new_data.to_csv(output, index=False, header=False)
                    output.seek(0)
                    conn = hook.get_conn()
                    cursor = conn.cursor()
                    cursor.copy_from(output, SOURCE_TABLE, sep=',')
                    conn.commit()
                    cursor.close()
                    logging.info(f"Appended {len(new_data)} new dates to {SOURCE_TABLE}.")
                 else:
                    logging.info("No new dates to insert into staging table.")
        else:
                logging.warning(f"Staging table {SOURCE_TABLE} is not empty but max_date is None. Skipping data insertion.")
        logging.info("Finished storing date dimension.")
        date_dim_df = hook.get_pandas_df('SELECT * FROM date_dim_3')
        print(f'this is test {date_dim_df}')
    except Exception as e:
        logging.error(f"Error in storing date dimension: {e}")
        
def _store_location_dim():
    try:
        logging.info('Starting to store location dimension.')
        temp_csv = '/tmp/filtered_location_dim.csv'
        if os.path.exists(temp_csv):
            os.remove(temp_csv)
        hook = PostgresHook(postgres_conn_id='postgres')
        
        # Read the location_dim.csv file
        location_dim_df = pd.read_csv('/tmp/location_dim.csv')
        
        location_dim_df.rename(columns={'RegionName': 'region_name'}, inplace=True)
        
        check_empty_query = "SELECT COUNT(*) FROM location_dim_3"
        count = hook.get_first(check_empty_query)[0]
        if count == 0:
        # Get existing region names from the location_dim_1 table
                location_dim_df.to_csv(temp_csv, index=False, header=True)
                # Copy the data into location_dim_3
                hook.copy_expert('''COPY location_dim_3(region_name) FROM STDIN WITH CSV DELIMITER ',' ''', filename=temp_csv)
                logging.info("Inserted all data into location_dim.")
        else:
            existing_regions=hook.get_pandas_df('SELECT region_name FROM  location_dim_3')['region_name'].tolist()
            new_regions_df= location_dim_df[~location_dim_df['region_name'].isin(existing_regions)]
            
            if not new_regions_df.empty:
                    new_regions_df.to_csv(temp_csv, index=False, header=False)
                    hook.copy_expert('''COPY location_dim_3(region_name) FROM STDIN WITH CSV DELIMITER ',' ''')
                    logging.info("Appended new regions to location_dim_3.")
            else:
                logging.info("No new regions to insert into location_dim_3.")          
        logging.info("Finished storing location dimension.")
        logging.info(f"all data{temp_csv}")
        
    except Exception as e:
        logging.error(f"Error in storing location dimension: {e}")

def _transform_combined_data():
    try:
        trnas_path ='/tmp/transformed_combined_data.csv'
        if os.path.exists(trnas_path):
            os.remove(trnas_path)
            
        combined_df = pd.read_csv('/tmp/combined_data.csv')
        # Read date_dim_3 from Postgres
        hook = PostgresHook(postgres_conn_id='postgres')
        date_dim_df = hook.get_pandas_df('SELECT * FROM date_dim_3')
        # Read location_dim_2 from Postgres
        location_dim_df = hook.get_pandas_df('SELECT * FROM location_dim_3')
        
        print("Columns in date_dim_df:", date_dim_df)
        print("columns in location", location_dim_df)
        
        combined_df['Date'] = pd.to_datetime(combined_df['Date'])
        date_dim_df['date'] = pd.to_datetime(date_dim_df['date'])
        # Join with date_dim to get DatePK
        combined_df = combined_df.merge(date_dim_df[['date', 'datepk']], 
                                        left_on='Date', right_on='date', 
                                        how='inner')        
        combined_df.rename(columns={'datepk': 'Date_FK'}, inplace=True)
        combined_df.drop(columns=['Date', 'date'], inplace=True)
        print('Combined data after date merge:')
        print(combined_df.head())

        combined_df['Country_Region'] = combined_df['Country_Region'].str.strip().str.lower()
        location_dim_df['region_name'] = location_dim_df['region_name'].str.strip().str.lower()
        # Join with location_dim to get Region_PK
        combined_df = combined_df.merge(location_dim_df[['region_name', 'region_pk']], 
                                        left_on='Country_Region', right_on='region_name', 
                                        how='inner')        
        combined_df.rename(columns={'region_pk': 'Region_FK'}, inplace=True)
        combined_df.drop(columns=['Country_Region', 'region_name'], inplace=True)
        combined_df.drop(['FIPS','Admin2','Province_State','Lat','Long_','Incident_Rate','Case_Fatality_Ratio'],axis=1,inplace=True)
        combined_df.fillna('Unkown',inplace=True)
        combined_df=combined_df[['Date_FK','Region_FK','Combined_Key','Confirmed','Active','Recovered','Deaths']]
    
        print('Combined data after location merge:')
        print(combined_df.head())
        
        
        # Save the transformed data
        combined_df.to_csv('/tmp/transformed_combined_data.csv', index=False, header=False)
        logging.info("Transformed combined data successfully.")
    except Exception as e:
        logging.error(f"Error in transforming combined data: {e}")
        
def _store_fact_cases():
            try:
                logging.info('start store fact_cases')
                hook = PostgresHook(postgres_conn_id='postgres')
                hook.copy_expert('''COPY fact_cases__(Date_FK, Region_FK, Combined_Key,Confirmed, Active,Deaths,Recovered) FROM STDIN WITH CSV HEADER DELIMITER AS ',' ''', filename='/tmp/transformed_combined_data.csv')
                print(hook.get_pandas_df('SELECT * FROM fact_cases__'))
                logging.info("Successfully stored fact cases")
            except Exception as e:
                logging.error(f"Error in storing fact cases: {e}")
        
        
with DAG('covid_data_pipeline', start_date=datetime(2025, 1, 7),schedule_interval='@daily', catchup=False, description='A simple pipeline to download, clean and save COVID data daily') as dag:
    
    create_date_dim=PostgresOperator(
        task_id='create_date_dim',
        postgres_conn_id='postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS date_dim_3 (
            DatePK VARCHAR(255) PRIMARY KEY,
            Date DATE,
            Day varchar(3),
            Month VARCHAR(3),
            Quarter INT,
            Year INT
            );
        '''  
    )
    
    create_location_dim=PostgresOperator(
        task_id='create_location_dim',
        postgres_conn_id='postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS location_dim_3 (
            Region_PK SERIAL  PRIMARY KEY,
            region_name VARCHAR(255)
            );
        '''  
    )
    
    create_fact=PostgresOperator(
        task_id='create_fact',
        postgres_conn_id='postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS fact_cases__ (
            Id SERIAL PRIMARY KEY,
            Date_FK VARCHAR(255),
            Region_FK INT,
            Combined_Key varchar(255),
            Confirmed varchar(9),
             Active varchar(9),
            Recovered varchar(9),
            Deaths varchar(9),
            constraint fk_date FOREIGN KEY(Date_FK) references date_dim_3(DatePK),
            constraint fk_region FOREIGN KEY(Region_FK) references location_dim_3(Region_PK)
            
        );
        '''  
    )
    
    # get_day opration
    download_task=PythonOperator(
     task_id='download_data',
        python_callable=get_files_from_github,
         provide_context=True,
     )
    
    merge_files=PythonOperator(
        task_id='merge_files',
        python_callable=merge_all_files,
        provide_context=True
    )
    
    get_date_dim=PythonOperator(
        task_id='get_date_dim',
        python_callable=_get_date_dim,
        provide_context=True
    )
    get_location_dim=PythonOperator(
        task_id='get_location_dim',
        python_callable=_get_location_dim,
        provide_context=True
    
    )
    
    store_date_dim=PythonOperator(
        task_id='store_date_dim',
        python_callable=_store_date_dim,
        provide_context=True
    )

    store_location_dim=PythonOperator(
        task_id='store_location_dim',
        python_callable=_store_location_dim,
        provide_context=True
    )
    get_transform_combined_data=PythonOperator(
        task_id='transform_combined_data',
        python_callable=_transform_combined_data
    )
    store_fact_cases=PythonOperator(
        task_id='store_fact_cases',
        python_callable=_store_fact_cases
    )
    
create_date_dim>> create_location_dim>> create_fact>> download_task>>merge_files>>get_date_dim>>get_location_dim>>store_date_dim>>store_location_dim>>get_transform_combined_data>>store_fact_cases
