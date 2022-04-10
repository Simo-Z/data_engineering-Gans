import requests
import pandas as pd
import datetime as dt
import sqlalchemy
import os
from dotenv import load_dotenv, find_dotenv

# load env data from .env file.
load_dotenv(find_dotenv(filename='.env'))

# GLOBAL Variables
OWM_API_KEY = os.environ["OWM_API_KEY"] 
schema = os.environ["DB_SCHEMA"] 
host = os.environ["DB_HOST"] 
user = os.environ["DB_USER"] 
password = os.environ["DB_PASSWORD"] 
port= os.environ["DB_PORT"] 
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'



# -------------------  GET CITIES  ------------------------------------------------------------------------------------

def get_cities(con):
    # Get all cities from DB

    # LIMIT FOR DEVELOPMENT ----------------<<<<<<<<<
    sql = '''
    SELECT * FROM cities
    ORDER BY city_pop desc
    limit 5;
    '''

    return pd.read_sql(sql, con)

# -------------------  GET CITY WEATHER  ------------------------------------------------------------------------------------

def get_city_weather(cities):
    current_df = pd.DataFrame()
    minutely_df = pd.DataFrame()
    hourly_df = pd.DataFrame()
    daily_df = pd.DataFrame()

    for index, city_row in cities.iterrows():
        url = f"https://api.openweathermap.org/data/2.5/onecall?lat={city_row['city_latitude']}&lon={city_row['city_longitude']}&appid={OWM_API_KEY}&units=metric"
        try:
            response = requests.get(url).json()
        except Exception as e:
            continue
        try:
            current = get_current_weather(response, city_row)
            current_df = pd.concat([current_df, current])
        except Exception as e:
            print("no current")
            
        try:
            minutely = get_minutely_weather(response, city_row)
            minutely_df = pd.concat([minutely_df, minutely])
        except Exception as e:
            print("no minutely")
        try:
            hourly = get_hourly_weather(response, city_row)
            hourly_df = pd.concat([hourly_df, hourly])
        except Exception as e:
            print("no hourly")
            
        try:

            daily = get_daily_weather(response, city_row)
            daily_df =pd.concat([daily_df, daily])
        except Exception as e:
            print("no daily")

            
    return [current_df, minutely_df, hourly_df, daily_df]



# -------------------  GET CURRENT WEATHER  ------------------------------------------------------------------------------------  


def get_current_weather(response, city_row):
    response["current"]["weather"] = response["current"]["weather"][0]
    current_weather = (
                pd.json_normalize(response["current"], sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return current_weather


# -------------------  GET MINUTELY WEATHER  ------------------------------------------------------------------------------------    

def get_minutely_weather(response, city_row):
    minutely_weather = (
                pd.json_normalize(response["minutely"], sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return minutely_weather

# -------------------  GET HOURLY WEATHER  ------------------------------------------------------------------------------------    

def get_hourly_weather(response, city_row):
    
    hourly_weather = (
                pd.json_normalize(response["hourly"], sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return hourly_weather

# -------------------  GET DAILY WEATHER  ------------------------------------------------------------------------------------    

def get_daily_weather(response, city_row):
    
    daily_weather = (
                pd.json_normalize(response["daily"], sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return daily_weather

# -------------------  SEND TO DB  ------------------------------------------------------------------------------------

def send_to_db(weather_df_list, con):      
    [current_weather, minutely_weather, hourly_weather, daily_weather] = weather_df_list 
    current_weather.to_sql(
        name = "current_weather", 
        con=con, 
        if_exists="replace",
        index=False,
        dtype={
            'municipality_country': sqlalchemy.types.VARCHAR(length=100),
        }
    )
    minutely_weather.to_sql(
        name = "minutely_weather", 
        con=con, 
        if_exists="replace",
        index=False,
        dtype={
            'municipality_country': sqlalchemy.types.VARCHAR(length=100),
        }
    )
    hourly_weather.to_sql(
        name = "hourly_weather", 
        con=con, 
        if_exists="replace",
        index=False,
        dtype={
            'municipality_country': sqlalchemy.types.VARCHAR(length=100),
        }
    )
    daily_weather.to_sql(
        name = "daily_weather", 
        con=con, 
        if_exists="replace",
        index=False,
        dtype={
            'municipality_country': sqlalchemy.types.VARCHAR(length=100),
        }
    )
    engine = sqlalchemy.create_engine(con)
    sql_fkey = '''
        ALTER TABLE {table_prefix}_weather 
        ADD FOREIGN KEY (municipality_country) REFERENCES cities(municipality_country);
        '''
    with engine.connect() as engine:
        # Add primary key?
        
        # Add foreign key
        engine.execute(sql_fkey.format(table_prefix = "current"))
        engine.execute(sql_fkey.format(table_prefix = "minutely"))
        engine.execute(sql_fkey.format(table_prefix = "hourly"))
        engine.execute(sql_fkey.format(table_prefix = "daily"))
        
    
# -------------------  LAMBDA HANDLER  ------------------------------------------------------------------------------------


def lambda_handler(event, context):

    # get List of Cities from all cities on DB
    cities = get_cities(con)

    # fetch City weather
    weather_df_list = get_city_weather(cities)
    # Create Tables on DB
    send_to_db(weather_df_list, con)

    event = ""
    context = ""
    
    lambda_handler(event, context)