# Retail Order data analysis using Python & Sql
## This project utilized a Python data pipeline to download and clean data from the Kaggle API. The prepared data was then ingested into an SQL database to facilitate further analysis. SQL queries were leveraged to answer five specific questions about the data.
![Screenshot 2024-05-25 184438](https://github.com/PRANAV7389/Python-SQL-Projects/assets/110465335/884f3dc0-eaf7-45fc-b2b9-e717d0141139)


# Data Pipeline to Download, Clean, and Load Data into SQL Database

## This Python script demonstrates a data pipeline that downloads a dataset from Kaggle, performs data cleaning, and loads the  prepared data into a SQL database for further analysis. Below are the detailed steps and the corresponding code.

### 1. Install Kaggle API



```python
# Install the Kaggle package to interact with the Kaggle API
!pip install kaggle
```

    Requirement already satisfied: kaggle in c:\users\dell\anaconda3\lib\site-packages (1.6.14)
    Requirement already satisfied: six>=1.10 in c:\users\dell\anaconda3\lib\site-packages (from kaggle) (1.16.0)
    Requirement already satisfied: certifi>=2023.7.22 in c:\users\dell\anaconda3\lib\site-packages (from kaggle) (2023.7.22)
    Requirement already satisfied: python-dateutil in c:\users\dell\anaconda3\lib\site-packages (from kaggle) (2.8.2)
    Requirement already satisfied: requests in c:\users\dell\anaconda3\lib\site-packages (from kaggle) (2.31.0)
    Requirement already satisfied: tqdm in c:\users\dell\anaconda3\lib\site-packages (from kaggle) (4.65.0)
    Requirement already satisfied: python-slugify in c:\users\dell\anaconda3\lib\site-packages (from kaggle) (5.0.2)
    Requirement already satisfied: urllib3 in c:\users\dell\anaconda3\lib\site-packages (from kaggle) (1.26.16)
    Requirement already satisfied: bleach in c:\users\dell\anaconda3\lib\site-packages (from kaggle) (4.1.0)
    Requirement already satisfied: packaging in c:\users\dell\anaconda3\lib\site-packages (from bleach->kaggle) (23.1)
    Requirement already satisfied: webencodings in c:\users\dell\anaconda3\lib\site-packages (from bleach->kaggle) (0.5.1)
    Requirement already satisfied: text-unidecode>=1.3 in c:\users\dell\anaconda3\lib\site-packages (from python-slugify->kaggle) (1.3)
    Requirement already satisfied: charset-normalizer<4,>=2 in c:\users\dell\anaconda3\lib\site-packages (from requests->kaggle) (2.0.4)
    Requirement already satisfied: idna<4,>=2.5 in c:\users\dell\anaconda3\lib\site-packages (from requests->kaggle) (3.4)
    Requirement already satisfied: colorama in c:\users\dell\anaconda3\lib\site-packages (from tqdm->kaggle) (0.4.6)
    

### 2. Download the Dataset from Kaggle



```python
import kaggle
# Download the 'orders.csv' file from the specified Kaggle dataset
!kaggle datasets download ankitbansal06/retail-orders -f orders.csv
```

    Dataset URL: https://www.kaggle.com/datasets/ankitbansal06/retail-orders
    License(s): CC0-1.0
    orders.csv.zip: Skipping, found more recently modified local copy (use --force to force download)
    

### 3. Extract the Downloaded ZIP File



```python
import zipfile
# Extract the downloaded zip file containing the 'orders.csv'
zip_ref = zipfile.ZipFile('orders.csv.zip')
zip_ref.extractall()
zip_ref.close()
```

### 4. Load Data into a DataFrame


```python
import pandas as pd
# Read the CSV file into a pandas DataFrame
df = pd.read_csv('orders.csv')
# Display the first 20 rows of the DataFrame
df.head(20)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Order Id</th>
      <th>Order Date</th>
      <th>Ship Mode</th>
      <th>Segment</th>
      <th>Country</th>
      <th>City</th>
      <th>State</th>
      <th>Postal Code</th>
      <th>Region</th>
      <th>Category</th>
      <th>Sub Category</th>
      <th>Product Id</th>
      <th>cost price</th>
      <th>List Price</th>
      <th>Quantity</th>
      <th>Discount Percent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2023-03-01</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Henderson</td>
      <td>Kentucky</td>
      <td>42420</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Bookcases</td>
      <td>FUR-BO-10001798</td>
      <td>240</td>
      <td>260</td>
      <td>2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2023-08-15</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Henderson</td>
      <td>Kentucky</td>
      <td>42420</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Chairs</td>
      <td>FUR-CH-10000454</td>
      <td>600</td>
      <td>730</td>
      <td>3</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2023-01-10</td>
      <td>Second Class</td>
      <td>Corporate</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90036</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Labels</td>
      <td>OFF-LA-10000240</td>
      <td>10</td>
      <td>10</td>
      <td>2</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2022-06-18</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Fort Lauderdale</td>
      <td>Florida</td>
      <td>33311</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Tables</td>
      <td>FUR-TA-10000577</td>
      <td>780</td>
      <td>960</td>
      <td>5</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2022-07-13</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Fort Lauderdale</td>
      <td>Florida</td>
      <td>33311</td>
      <td>South</td>
      <td>Office Supplies</td>
      <td>Storage</td>
      <td>OFF-ST-10000760</td>
      <td>20</td>
      <td>20</td>
      <td>2</td>
      <td>5</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>2022-03-13</td>
      <td>Not Available</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90032</td>
      <td>West</td>
      <td>Furniture</td>
      <td>Furnishings</td>
      <td>FUR-FU-10001487</td>
      <td>50</td>
      <td>50</td>
      <td>7</td>
      <td>3</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>2022-12-28</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90032</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Art</td>
      <td>OFF-AR-10002833</td>
      <td>10</td>
      <td>10</td>
      <td>4</td>
      <td>3</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>2022-01-25</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90032</td>
      <td>West</td>
      <td>Technology</td>
      <td>Phones</td>
      <td>TEC-PH-10002275</td>
      <td>860</td>
      <td>910</td>
      <td>6</td>
      <td>5</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>2023-03-23</td>
      <td>Not Available</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90032</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Binders</td>
      <td>OFF-BI-10003910</td>
      <td>20</td>
      <td>20</td>
      <td>3</td>
      <td>2</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>2023-05-16</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90032</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Appliances</td>
      <td>OFF-AP-10002892</td>
      <td>90</td>
      <td>110</td>
      <td>5</td>
      <td>3</td>
    </tr>
    <tr>
      <th>10</th>
      <td>11</td>
      <td>2023-03-31</td>
      <td>Not Available</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90032</td>
      <td>West</td>
      <td>Furniture</td>
      <td>Tables</td>
      <td>FUR-TA-10001539</td>
      <td>1470</td>
      <td>1710</td>
      <td>9</td>
      <td>3</td>
    </tr>
    <tr>
      <th>11</th>
      <td>12</td>
      <td>2023-12-25</td>
      <td>Not Available</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90032</td>
      <td>West</td>
      <td>Technology</td>
      <td>Phones</td>
      <td>TEC-PH-10002033</td>
      <td>750</td>
      <td>910</td>
      <td>4</td>
      <td>3</td>
    </tr>
    <tr>
      <th>12</th>
      <td>13</td>
      <td>2022-02-11</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Concord</td>
      <td>North Carolina</td>
      <td>28027</td>
      <td>South</td>
      <td>Office Supplies</td>
      <td>Paper</td>
      <td>OFF-PA-10002365</td>
      <td>20</td>
      <td>20</td>
      <td>3</td>
      <td>3</td>
    </tr>
    <tr>
      <th>13</th>
      <td>14</td>
      <td>2023-07-18</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Seattle</td>
      <td>Washington</td>
      <td>98103</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Binders</td>
      <td>OFF-BI-10003656</td>
      <td>360</td>
      <td>410</td>
      <td>3</td>
      <td>2</td>
    </tr>
    <tr>
      <th>14</th>
      <td>15</td>
      <td>2023-11-09</td>
      <td>unknown</td>
      <td>Home Office</td>
      <td>United States</td>
      <td>Fort Worth</td>
      <td>Texas</td>
      <td>76106</td>
      <td>Central</td>
      <td>Office Supplies</td>
      <td>Appliances</td>
      <td>OFF-AP-10002311</td>
      <td>60</td>
      <td>70</td>
      <td>5</td>
      <td>5</td>
    </tr>
    <tr>
      <th>15</th>
      <td>16</td>
      <td>2022-06-18</td>
      <td>Standard Class</td>
      <td>Home Office</td>
      <td>United States</td>
      <td>Fort Worth</td>
      <td>Texas</td>
      <td>76106</td>
      <td>Central</td>
      <td>Office Supplies</td>
      <td>Binders</td>
      <td>OFF-BI-10000756</td>
      <td>0</td>
      <td>0</td>
      <td>3</td>
      <td>5</td>
    </tr>
    <tr>
      <th>16</th>
      <td>17</td>
      <td>2022-02-04</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Madison</td>
      <td>Wisconsin</td>
      <td>53711</td>
      <td>Central</td>
      <td>Office Supplies</td>
      <td>Storage</td>
      <td>OFF-ST-10004186</td>
      <td>610</td>
      <td>670</td>
      <td>6</td>
      <td>3</td>
    </tr>
    <tr>
      <th>17</th>
      <td>18</td>
      <td>2023-08-04</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>West Jordan</td>
      <td>Utah</td>
      <td>84084</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Storage</td>
      <td>OFF-ST-10000107</td>
      <td>60</td>
      <td>60</td>
      <td>2</td>
      <td>4</td>
    </tr>
    <tr>
      <th>18</th>
      <td>19</td>
      <td>2022-01-23</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>San Francisco</td>
      <td>California</td>
      <td>94109</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Art</td>
      <td>OFF-AR-10003056</td>
      <td>10</td>
      <td>10</td>
      <td>2</td>
      <td>4</td>
    </tr>
    <tr>
      <th>19</th>
      <td>20</td>
      <td>2022-01-11</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>San Francisco</td>
      <td>California</td>
      <td>94109</td>
      <td>West</td>
      <td>Technology</td>
      <td>Phones</td>
      <td>TEC-PH-10001949</td>
      <td>170</td>
      <td>210</td>
      <td>3</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



### 5. Data Inspection


```python
# Display information about the DataFrame (column types, non-null values, etc.)
df.info()
# Check for missing values in the DataFrame
df.isnull().sum()
# Display unique values in the 'Ship Mode' column
df['ship_mode'].unique
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 9994 entries, 0 to 9993
    Data columns (total 16 columns):
     #   Column        Non-Null Count  Dtype         
    ---  ------        --------------  -----         
     0   order_id      9994 non-null   int64         
     1   order_date    9994 non-null   datetime64[ns]
     2   ship_mode     9988 non-null   object        
     3   segment       9994 non-null   object        
     4   country       9994 non-null   object        
     5   city          9994 non-null   object        
     6   state         9994 non-null   object        
     7   postal_code   9994 non-null   int64         
     8   region        9994 non-null   object        
     9   category      9994 non-null   object        
     10  sub_category  9994 non-null   object        
     11  product_id    9994 non-null   object        
     12  quantity      9994 non-null   int64         
     13  discount      9994 non-null   float64       
     14  sale_price    9994 non-null   float64       
     15  profit        9994 non-null   float64       
    dtypes: datetime64[ns](1), float64(3), int64(3), object(9)
    memory usage: 1.2+ MB
    




    <bound method Series.unique of 0         Second Class
    1         Second Class
    2         Second Class
    3       Standard Class
    4       Standard Class
                 ...      
    9989      Second Class
    9990    Standard Class
    9991    Standard Class
    9992    Standard Class
    9993      Second Class
    Name: ship_mode, Length: 9994, dtype: object>



### 6. Data Cleaning


```python
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
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>order_date</th>
      <th>ship_mode</th>
      <th>segment</th>
      <th>country</th>
      <th>city</th>
      <th>state</th>
      <th>postal_code</th>
      <th>region</th>
      <th>category</th>
      <th>sub_category</th>
      <th>product_id</th>
      <th>cost_price</th>
      <th>list_price</th>
      <th>quantity</th>
      <th>discount_percent</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2023-03-01</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Henderson</td>
      <td>Kentucky</td>
      <td>42420</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Bookcases</td>
      <td>FUR-BO-10001798</td>
      <td>240</td>
      <td>260</td>
      <td>2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2023-08-15</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Henderson</td>
      <td>Kentucky</td>
      <td>42420</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Chairs</td>
      <td>FUR-CH-10000454</td>
      <td>600</td>
      <td>730</td>
      <td>3</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2023-01-10</td>
      <td>Second Class</td>
      <td>Corporate</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90036</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Labels</td>
      <td>OFF-LA-10000240</td>
      <td>10</td>
      <td>10</td>
      <td>2</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2022-06-18</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Fort Lauderdale</td>
      <td>Florida</td>
      <td>33311</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Tables</td>
      <td>FUR-TA-10000577</td>
      <td>780</td>
      <td>960</td>
      <td>5</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2022-07-13</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Fort Lauderdale</td>
      <td>Florida</td>
      <td>33311</td>
      <td>South</td>
      <td>Office Supplies</td>
      <td>Storage</td>
      <td>OFF-ST-10000760</td>
      <td>20</td>
      <td>20</td>
      <td>2</td>
      <td>5</td>
    </tr>
  </tbody>
</table>
</div>



### 7. Feature Engineering


```python
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
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>order_id</th>
      <th>order_date</th>
      <th>ship_mode</th>
      <th>segment</th>
      <th>country</th>
      <th>city</th>
      <th>state</th>
      <th>postal_code</th>
      <th>region</th>
      <th>category</th>
      <th>sub_category</th>
      <th>product_id</th>
      <th>quantity</th>
      <th>discount</th>
      <th>sale_price</th>
      <th>profit</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2023-03-01</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Henderson</td>
      <td>Kentucky</td>
      <td>42420</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Bookcases</td>
      <td>FUR-BO-10001798</td>
      <td>2</td>
      <td>5.2</td>
      <td>254.8</td>
      <td>14.8</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2023-08-15</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Henderson</td>
      <td>Kentucky</td>
      <td>42420</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Chairs</td>
      <td>FUR-CH-10000454</td>
      <td>3</td>
      <td>21.9</td>
      <td>708.1</td>
      <td>108.1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2023-01-10</td>
      <td>Second Class</td>
      <td>Corporate</td>
      <td>United States</td>
      <td>Los Angeles</td>
      <td>California</td>
      <td>90036</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Labels</td>
      <td>OFF-LA-10000240</td>
      <td>2</td>
      <td>0.5</td>
      <td>9.5</td>
      <td>-0.5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2022-06-18</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Fort Lauderdale</td>
      <td>Florida</td>
      <td>33311</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Tables</td>
      <td>FUR-TA-10000577</td>
      <td>5</td>
      <td>19.2</td>
      <td>940.8</td>
      <td>160.8</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2022-07-13</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Fort Lauderdale</td>
      <td>Florida</td>
      <td>33311</td>
      <td>South</td>
      <td>Office Supplies</td>
      <td>Storage</td>
      <td>OFF-ST-10000760</td>
      <td>2</td>
      <td>1.0</td>
      <td>19.0</td>
      <td>-1.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>9989</th>
      <td>9990</td>
      <td>2023-02-18</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Miami</td>
      <td>Florida</td>
      <td>33180</td>
      <td>South</td>
      <td>Furniture</td>
      <td>Furnishings</td>
      <td>FUR-FU-10001889</td>
      <td>3</td>
      <td>1.2</td>
      <td>28.8</td>
      <td>-1.2</td>
    </tr>
    <tr>
      <th>9990</th>
      <td>9991</td>
      <td>2023-03-17</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Costa Mesa</td>
      <td>California</td>
      <td>92627</td>
      <td>West</td>
      <td>Furniture</td>
      <td>Furnishings</td>
      <td>FUR-FU-10000747</td>
      <td>2</td>
      <td>3.6</td>
      <td>86.4</td>
      <td>16.4</td>
    </tr>
    <tr>
      <th>9991</th>
      <td>9992</td>
      <td>2022-08-07</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Costa Mesa</td>
      <td>California</td>
      <td>92627</td>
      <td>West</td>
      <td>Technology</td>
      <td>Phones</td>
      <td>TEC-PH-10003645</td>
      <td>2</td>
      <td>5.2</td>
      <td>254.8</td>
      <td>34.8</td>
    </tr>
    <tr>
      <th>9992</th>
      <td>9993</td>
      <td>2022-11-19</td>
      <td>Standard Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Costa Mesa</td>
      <td>California</td>
      <td>92627</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Paper</td>
      <td>OFF-PA-10004041</td>
      <td>4</td>
      <td>0.9</td>
      <td>29.1</td>
      <td>-0.9</td>
    </tr>
    <tr>
      <th>9993</th>
      <td>9994</td>
      <td>2022-07-17</td>
      <td>Second Class</td>
      <td>Consumer</td>
      <td>United States</td>
      <td>Westminster</td>
      <td>California</td>
      <td>92683</td>
      <td>West</td>
      <td>Office Supplies</td>
      <td>Appliances</td>
      <td>OFF-AP-10002684</td>
      <td>2</td>
      <td>7.2</td>
      <td>232.8</td>
      <td>22.8</td>
    </tr>
  </tbody>
</table>
<p>9994 rows × 16 columns</p>
</div>



### 8. Load Data into SQL Database


```python
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
```

# SQL Query to analyze the data:



```python
Select all records from the df_orders table
SELECT * FROM pranav.df_orders;

-- Find top 10 highest revenue-generating products
-- 1. Group the records by product_id
-- 2. Calculate the sum of sale_price for each product_id
-- 3. Order the results by sales in descending order
-- 4. Limit the results to the top 10 entries
SELECT product_id, sum(sale_price) as sales
FROM df_orders
GROUP BY product_id
ORDER BY sales DESC
LIMIT 10;

-- Find top 5 highest selling products in each region
-- 1. Create a common table expression (CTE) to calculate sales per product_id for each region
-- 2. Use ROW_NUMBER() to rank the products within each region by sales in descending order
-- 3. Select the top 5 products (rn <= 5) for each region
WITH cte AS (
  SELECT region, product_id, sum(sale_price) as sales
  FROM df_orders
  GROUP BY region, product_id
)
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY region ORDER BY sales DESC) as rn
  FROM cte
) A
WHERE rn <= 5;

-- Find month-over-month growth comparison for 2022 and 2023 sales
-- 1. Create a CTE to calculate the total sales per month for each year
-- 2. Use CASE statements to segregate sales data for 2022 and 2023
-- 3. Group the results by month and order by month
WITH cte AS (
  SELECT YEAR(order_date) as year, MONTH(order_date) as month, sum(sale_price) as sales
  FROM df_orders
  GROUP BY YEAR(order_date), MONTH(order_date)
)
SELECT month,
  sum(CASE WHEN year = 2022 THEN sales ELSE 0 END) as sales_2022,
  sum(CASE WHEN year = 2023 THEN sales ELSE 0 END) as sales_2023
FROM cte
GROUP BY month
ORDER BY month;

-- For each category, find the month with the highest sales
-- 1. Create a CTE to calculate sales per category for each year-month
-- 2. Use ROW_NUMBER() to rank the months within each category by sales in descending order
-- 3. Select the month with the highest sales (rn = 1) for each category
WITH cte AS (
  SELECT category, FORMAT(order_date, 'yyyyMM') AS order_year_month, sum(sale_price) as sales
  FROM df_orders
  GROUP BY category, FORMAT(order_date, 'yyyyMM')
)
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY category ORDER BY sales DESC) as rn
  FROM cte
) a
WHERE rn = 1;

-- Identify which sub-category had the highest growth by profit in 2023 compared to 2022
-- 1. Create a CTE to calculate sales per sub_category for each year
-- 2. Create another CTE to calculate the growth by profit between 2023 and 2022 for each sub_category
-- 3. Order the results by growth in descending order and limit to the top 1 entry
WITH cte AS (
  SELECT sub_category, YEAR(order_date) as year, sum(sale_price) as sales
  FROM df_orders
  GROUP BY sub_category, YEAR(order_date)
), cte2 AS (
  SELECT sub_category,
    sum(CASE WHEN year = 2022 THEN sales ELSE 0 END) as sales_2022,
    sum(CASE WHEN year = 2023 THEN sales ELSE 0 END) as sales_2023
  FROM cte
  GROUP BY sub_category
)
SELECT *,
  (sales_2023 - sales_2022) * 100 / sales_2022 as growth_percentage
FROM cte2
ORDER BY growth_percentage DESC
LIMIT 1;
```


```python

```


