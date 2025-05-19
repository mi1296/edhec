# ETL Pipeline for Financial Time Series Data

""" **Madhumitha ICHAPURAM - M1 Data Science and AI for Business**

1. Extracts intraday data from a financial API (e.g., Alpha Vantage)
2. Transforms the data: renaming, type conversion, cleaning
3. Loads the data into a local SQLite database
"""

# Import required libraries for API access, data manipulation, and database interaction
import requests
import pandas as pd
from sqlalchemy import create_engine, text

"""
### EXTRACT STEP

Set up the API request to Alpha Vantage:
- Define API key, stock symbol, and other query parameters
- Fetch the intraday time series data as JSON
"""

API_KEY = "YOUR_API_KEY"  # Replace with your actual API key
# API configuration
SYMBOL = "IBM"
FUNCTION = "TIME_SERIES_INTRADAY"
INTERVAL = "1min"
BASE_URL = "https://www.alphavantage.co/query"

url = f"{BASE_URL}?function={FUNCTION}&symbol={SYMBOL}&interval={INTERVAL}&apikey={API_KEY}"

# API request
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    api_data_1 = response.json()
else:
    print(f"Error: Unable to fetch data. HTTP Status Code: {response.status_code}")

response.status_code

# Check the type of api_data_1
type(api_data_1)

'''
after confirming the data is a dictionary, we can check the keys 
to see the structure of the data
'''
api_data_1.keys()

'''
The data is nested, so we need to extract the relevant part
The relevant part is under the key "Time Series (1min)"
'''
# Extracting the time series data
df = pd.DataFrame(api_data_1["Time Series (1min)"]).T
df.head()

'''
The columns could be better named for easier access.
The datetime column is the index, and this might bring errors while loading into the database.
We need to reset the index and rename the columns.
We can also convert the index to a datetime object for better handling of time series data.
We will also convert the columns to numeric types for easier manipulation.
We can rename the columns to make them more user-friendly.
'''

# rename columns
df.columns = ["open", "high", "low", "close", "volume"]
# reset index
df.reset_index(inplace=True)
# rename index column
df.rename(columns={"index": "timestamp"}, inplace=True)
# convert timestamp to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])

df.head()

'''
After improving our column names, we still have to ensure that the data types of each column 
are correctly assigned. 
'''

df.dtypes

'''Since we are going to load this data into a database,
we need to ensure that the data types are compatible with the database schema.
We can convert the columns to numeric types using the `pd.to_numeric` function.
We can also set the `errors` parameter to "coerce" to handle any non-numeric values by converting them to NaN.
We can also check the data types of the columns to ensure they are correct.
'''

df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
df.dtypes

'''
We can also check for any missing values in the data.
We can use the `isnull` method to check for missing values in the DataFrame.
We can use the `sum` method to count the number of missing values in each column.
'''

df.isnull().sum()

'''
Now that we have cleaned the data, we can load it into a database.
Here we are using SQLite as the database engine,
but you can use any other database engine supported by SQLAlchemy.
We can also use the `if_exists` parameter to specify what to do if the table already exists.
We can set it to "replace" to drop the existing table and create a new one,
or "append" to add the new data to the existing table.
We can also set the `index` parameter to False to avoid writing the index to the database since
we already have a timestamp column.
'''

# Create a SQLite database engine
engine = create_engine(f"sqlite:///time_series_intraday_{SYMBOL.lower()}.db", future=True)
# Load the DataFrame into the database
df.to_sql(f"time_series_intraday_{SYMBOL.lower()}", con=engine, if_exists="replace", index=False)

# Preview a few rows from the final database table
with engine.connect() as conn:
    result = conn.execute(text(f"SELECT * FROM time_series_intraday_{SYMBOL.lower()} LIMIT 5"))
    for row in result:
        print(row)
