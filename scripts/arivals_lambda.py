import requests
import pandas as pd
import datetime as dt
import sqlalchemy

# GLOBAL Variables
schema="gans"
host="gans-aws.cs3d3b90junp.us-east-1.rds.amazonaws.com"
user="admin"
password = "pEjhiw-wygsy4-quhsos"
port=3306
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

def lambda_handler(event, context):
    # get List of Airports from DB
    airports = get_airport_list(con)
    # fetch City weather for every city with a population > 100.000
    [init_arrivals, init_departures] = get_arrivals_df(airports)
    arrivals = clean_arrivals_df(init_arrivals)
    departures = clean_arrivals_df(init_departures)
    send_to_db(df=arrivals, table_name = "airport_arrivals", if_exists="replace");
    send_to_db(df=departures, table_name = "airport_departures", if_exists="replace");
    

def get_airport_list(con):
    # Berlin Airports
    sql = '''
    SELECT * FROM airports
    WHERE municipality_country = "Berlin,DE";
    '''

    return pd.read_sql(sql, con)


def get_arrivals_df(airports):
    for index, airport_row in airports.iterrows():
        url = "https://aerodatabox.p.rapidapi.com/flights/airports/icao/EDDB/2022-04-07T19:00/2022-04-08T07:00"
        querystring = {"withLeg":"true","direction":"Both","withCancelled":"true","withCodeshared":"true","withCargo":"true","withPrivate":"true","withLocation":"true"}
        headers = {
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
            "X-RapidAPI-Key": "c1bc1a1acemsh99ced7306b1d2c9p1c10d7jsn078f60dc1a0e"
        }
        try:
            response = requests.request("GET", url, headers=headers, params=querystring).json()

            departures = pd.json_normalize(response["departures"])
            departures["municipality_country"] = airport_row["municipality_country"] 

            arrivals = pd.json_normalize(response["arrivals"])
            arrivals["municipality_country"] = airport_row["municipality_country"] 

        except Exception as e:
            continue
    return [departures, arrivals]

def clean_arrivals_df(df):
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
    return df

def get_current_city_weather(current_response, city_row):
    current_response["weather"] = current_response["weather"][0]
    city_current_weather = (
                pd.json_normalize(current_response, sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return city_current_weather