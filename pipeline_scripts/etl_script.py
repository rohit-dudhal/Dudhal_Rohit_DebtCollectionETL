import pandas as pd
from utils.connectors import mysql_connect_to_db
from utils.create_schema import borrower_dtype
import logging
import os
from datetime import datetime
import json

# Create a folder for today's date
today_date = datetime.now().strftime('%Y-%m-%d')
log_folder = os.path.join('pipeline_scripts', 'logs', today_date)
os.makedirs(log_folder, exist_ok=True)

# Configure logging to write to a file in the dated folder
log_config_file_path = os.path.join(log_folder, 'etl_script.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_config_file_path), logging.StreamHandler()])

def extract_data():
    """
    Reads a CSV file and returns a DataFrame.
    
    Parameters:
    file_path (str): The path to the CSV file.
    
    Returns:
    pd.DataFrame: DataFrame containing the CSV data.
    """
    file_path = 'pipeline_scripts/dataset/5k_borrowers_data.csv'

    if not os.path.isfile(file_path):
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        logging.info(f"Successfully read the file: {file_path}")
        return df
    
    except pd.errors.EmptyDataError:
        logging.error(f"File is empty: {file_path}")
        raise pd.errors.EmptyDataError(f"File is empty: {file_path}")

    except Exception as e:
        logging.error(f"An error occurred while reading the file: {file_path} - {str(e)}")
        raise Exception(f"An error occurred while reading the file: {file_path} - {str(e)}")

def transform(data):
    # Rename the columns: replace spaces with underscores and convert to lowercase
    data.columns = [col.replace(' ', '_').lower() for col in data.columns]

    # Convert 'Date of Birth' to datetime format
    data['date_of_birth'] = pd.to_datetime(data['date_of_birth'], format='%d-%m-%Y')

    # Standardize 'Mailing Address' formatting (e.g., remove extra spaces, correct common abbreviations)
    data['mailing_address'] = data['mailing_address'].str.strip().str.replace(r'\s+', ' ', regex=True)

    # Separate 'Geolocation' into 'Latitude' and 'Longitude'
    data[['lattitude', 'longitude']] = data['geolocation'].str.split(',', expand=True).astype(float)

    # Extract the last six-digit pincode using a vectorized string operation
    data['pincode'] = data['mailing_address'].str.extract(r'(\d{6})$')

    # Parse 'Repayment History'
    def parse_repayment_history(history):
        try:
            history = history.replace('datetime.date', '').replace('(', '"').replace(')', '"')
            repayment_list = json.loads(history.replace("'", '"'))
            return(repayment_list)
            
        except:
            return None

    def count_key_value_pairs(repayment_list):
        if isinstance(repayment_list, list):
            return(sum(len(entry) for entry in repayment_list)//2)
        return 0
    
    data['Repayment History Parsed'] = data['repayment_history'].apply(parse_repayment_history)
    data['total_emi_paid_count'] = data['Repayment History Parsed'].apply(count_key_value_pairs)
    data.drop(columns=['Repayment History Parsed','geolocation'], inplace=True)
    return data
def load(data):
    conn = mysql_connect_to_db('production')
    data.to_sql('borrower', conn, if_exists='replace', index=False, dtype=borrower_dtype)

    
df = extract_data()
data = transform(df)
load(data)