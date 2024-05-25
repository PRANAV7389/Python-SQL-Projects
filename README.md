# Retail Order data analysis using Python & Sql
## This project utilized a Python data pipeline to download and clean data from the Kaggle API. The prepared data was then ingested into an SQL database to facilitate further analysis. SQL queries were leveraged to answer five specific questions about the data.
![Screenshot 2024-05-25 184438](https://github.com/PRANAV7389/Python-SQL-Projects/assets/110465335/884f3dc0-eaf7-45fc-b2b9-e717d0141139)


## Data Pipeline to Download, Clean, and Load Data into SQL Database
This Python script demonstrates a data pipeline that downloads a dataset from Kaggle, performs data cleaning, and loads the prepared data into a SQL database for further analysis. Below are the detailed steps and the corresponding code.

Prerequisites
Python 3.x
Kaggle API credentials
MySQL server
Steps and Code
1. Install Kaggle API
python
Copy code
# Install the Kaggle package to interact with the Kaggle API
!pip install kaggle
2. Download the Dataset from Kaggle
python
Copy code
import kaggle
# Download the 'orders.csv' file from the specified Kaggle dataset
!kaggle datasets download ankitbansal06/retail-orders -f orders.csv
3. Extract the Downloaded ZIP File
python
Copy code
import zipfile
# Extract the downloaded zip file containing the 'orders.csv'
zip_ref = zipfile.ZipFile('orders.csv.zip')
zip_ref.extractall()
zip_ref.close()
4. Load Data into a DataFrame
python
Copy code
import pandas as pd
# Read the CSV file into a pandas DataFrame
df = pd.read_csv('orders.csv')
# Display the first 20 rows of the DataFrame
df.head(20)
5. Data Inspection
python
Copy code
# Display information about the DataFrame (column types, non-null values, etc.)
df.info()
# Check for missing values in the DataFrame
df.isnull().sum()
# Display unique values in the 'Ship Mode' column
df['ship_mode'].unique()
6. Data Cleaning
python
Copy code
# Re-read the CSV file, treating 'Not Available' and 'unknown' as NaN
df = pd.read_csv('orders.csv', na_values=['Not Available', 'unknown'])
df.head(20)
# Convert column names to lowercase
df.columns = df.columns.str.lower()
df.columns
# Replace spaces in column names with underscores
df.columns = df.columns.str.replace(' ', '_')
df.columns
df.head(5)
7. Feature Engineering
python
Copy code
# Calculate discount amount
df['discount'] = df['list_price'] * df['discount_percent'] * 0.01
# Calculate sale price after discount
df['sale_price'] = df['list_price'] - df['discount']
df
# Calculate profit
df['profit'] = df['sale_price'] - df['cost_price']
# Check data types
df.dtypes
# Convert 'order_date' column to datetime format
df['order_date'] = pd.to_datetime(df['order_date'], format="%Y-%m-%d")
df.dtypes
# Drop unnecessary columns
df.drop(columns=['list_price', 'cost_price', 'discount_percent'], inplace=True)
df
8. Load Data into SQL Database
python
Copy code
from sqlalchemy import create_engine
import sqlalchemy as sal

# Database connection details
user = 'pranav_user'
password = 'kshama1234'
host = 'localhost'
database = 'pranav'

# Create a SQLAlchemy engine
engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
conn = engine.connect()

# Load the DataFrame into the SQL database table 'df_orders'
df.to_sql('df_orders', con=conn, index=False, if_exists='append')
