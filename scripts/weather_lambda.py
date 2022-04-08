import requests
import pandas as pd
import datetime as dt
import sqlalchemy

# GLOBAL Variables
API_KEY = "c21a13f270f9dfc33b7894b574d754ad"
schema="gans"
host="gans-aws.cs3d3b90junp.us-east-1.rds.amazonaws.com"
user="admin"
password = "pEjhiw-wygsy4-quhsos"
port=3306
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

def lambda_handler(event, context):
    # get List of Cities from all cities on DB
    cities = get_cities(con)
    # fetch City weather fir every city with a population > 100.000
    init_city_weather_df = get_city_weather(cities)
    city_weather_df = clean_city_weather(init_city_weather_df)
    city_weather_df.assign(created_at = event["time"]).to_sql('city_weather', con=con, if_exists="replace", index=False)
    send_to_db(df=city_weather_df, table_name = "city_weather", if_exists="replace");
    

def get_cities(con):
    # All citys with a population > 100.000 
    sql = '''
    SELECT municipality_country, city_name, city_latitude, city_longitude FROM cities
    WHERE city_pop > 100000
    ORDER BY city_pop desc
    limit 3;
    '''

    return pd.read_sql(sql, con)


def get_city_weather(cities):
    current_weather_df = pd.DataFrame()
    for index, city_row in cities.iterrows():
        API_KEY = "c21a13f270f9dfc33b7894b574d754ad"
        url = f"https://api.openweathermap.org/data/2.5/onecall?lat={city_row['city_latitude']}&lon={city_row['city_longitude']}&appid={API_KEY}&units=metric"
        try:
            response = requests.get(url).json()
            current_city_weather = get_current_city_weather(response["current"], city_row)
            pd.concat([current_weather_df, current_city_weather])
                
        except Exception as e:
            continue
    return current_weather_df

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
    return df

def get_current_city_weather(current_response, city_row):
    current_response["weather"] = current_response["weather"][0]
    city_current_weather = (
                pd.json_normalize(current_response, sep="_")
                .assign(municipality_country = city_row["municipality_country"])
            ) 
    return city_current_weather