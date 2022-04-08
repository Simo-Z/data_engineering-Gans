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
port=os.environ["DB_POrt"] 
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

# -------------------  LAMBDA HANDLER  ------------------------------------------------------------------------------------


def lambda_handler(event, context):
    # get List of Cities from all cities on DB
    cities = get_cities(con)
    # fetch City weather fir every city with a population > 100.000
    [current_df, minutely_df, hourly_df, daily_df] = get_city_weather(cities)

    # Create Tables on DB
    current_df.sql("current_weather", con=con, if_exists="replace", index=False)
    minutely_df.to_sql("minutely_weather", con=con, if_exists="replace", index=False)
    hourly_df.to_sql("hourly_weather", con=con, if_exists="replace", index=False)
    daily_df.to_sql("daily_weather", con=con, if_exists="replace", index=False)
    

# -------------------  GET CITIES  ------------------------------------------------------------------------------------

def get_cities(con):
    # Get all cities from DB

    # LIMIT FOR DEVELOPMENT ----------------<<<<<<<<<
    sql = '''
    SELECT * FROM cities
    ORDER BY city_pop desc
    limit 2;
    '''

    return pd.read_sql(sql, con)

# -------------------  GET CITY WEATHER  ------------------------------------------------------------------------------------

def get_city_weather(cities):
    current_weather_df = pd.DataFrame()
    minutely_weather_df = pd.DataFrame()
    hourly_weather_df = pd.DataFrame()
    daily_weather_df = pd.DataFrame()

    for index, city_row in cities.iterrows():
        url = f"https://api.openweathermap.org/data/2.5/onecall?lat={city_row['city_latitude']}&lon={city_row['city_longitude']}&appid={OWM_API_KEY}&units=metric"
        try:
            response = requests.get(url).json() 
            current_weather = get_current_weather(response, city_row)
            minutely_weather = get_minutely_weather(response, city_row)
            hourly_weather = get_hourly_weather(response, city_row)
            daily_weather = get_daily_weather(response, city_row)

            current_weather_df = pd.concat([current_weather_df, current_weather])
            minutely_weather_df = pd.concat([minutely_weather_df, minutely_weather])
            hourly_weather_df = pd.concat([hourly_weather_df, hourly_weather])
            daily_weather_df =pd.concat([daily_weather_df, daily_weather])
                
        except Exception as e:
            continue
    return [current_weather_df, minutely_weather_df, hourly_weather_df, daily_weather_df]

# -------------------  CLEAN CURRENT WEATHER  ------------------------------------------------------------------------------------

def clean_city_weather(df):
    return df

def send_to_db(df, table_name, if_exists="replace"):      
    df.to_sql(
        table_name, 
        con=con, 
        if_exists=if_exists,
        index=False,
        dtype={
             'temp': sqlalchemy.types.SmallInteger(),
             'feels_like': sqlalchemy.types.SmallInteger(),
             'temp_min': sqlalchemy.types.SmallInteger(),
             'temp_max': sqlalchemy.types.SmallInteger(),
        #     'city_name': sqlalchemy.types.VARCHAR(length=40),
        #     'city_country': sqlalchemy.types.VARCHAR(length=40),
        #     'city_longitude': sqlalchemy.types.Float(precision=3, asdecimal=True),
        #     'city_latitude': sqlalchemy.types.Float(precision=3, asdecimal=True),
              'municipality_country': sqlalchemy.types.VARCHAR(length=100),
        #     'created_at': sqlalchemy.types.DateTime(),
        #     'city_pop': sqlalchemy.types.Integer()
        }
    )
    engine = sqlalchemy.create_engine(con)
    with engine.connect() as engine:
        # Add primary key
        engine.execute('''
        ALTER TABLE city_weather 
        ADD PRIMARY KEY (municipality_country);
        '''
        )
        # Add foreign key
        engine.execute('''
        ALTER TABLE city_weather 
        ADD FOREIGN KEY (municipality_country) REFERENCES cities(municipality_country);
        ''')


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
    response["minutely"]["weather"] = response["minutely"]["weather"][0]
    minutely_weather = (
                pd.json_normalize(response["minutely"], sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return minutely_weather

# -------------------  GET HOURLY WEATHER  ------------------------------------------------------------------------------------    

def get_hourly_weather(response, city_row):
    response["hourly"]["weather"] = response["hourly"]["weather"][0]
    hourly_weather = (
                pd.json_normalize(response["hourly"], sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return hourly_weather

# -------------------  GET DAILY WEATHER  ------------------------------------------------------------------------------------    

def get_daily_weather(response, city_row):
    response["daily"]["weather"] = response["daily"]["weather"][0]
    daily_weather = (
                pd.json_normalize(response["daily"], sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return daily_weather

