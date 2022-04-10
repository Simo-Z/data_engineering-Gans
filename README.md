# Data Engineering with Python, AWS Lambda, AWS Event Bridge and MYSQL

## Description:

I started this project to gain a better understanding for data pipelines and to get some experience in automating data collection.
Starting from a JSON file from [Open Weather map](https://openweathermap.org) with information about the cities it covers and a CSV with airport information, a relational database was populated. With AWS Lambdas and AWS Event Bridge, the Open Weather API, as well as the [AeroboxData API](https://www.aerodatabox.com) are used to provide regular weather information, as well as arrivals and departures from German cities. The data is collected in cloud based relational Data Base and all the tables are connected with primary and foreign keys.

![My Database Schema](/assets/Schema.png)

## Features:

- Daily weather + forecast information for german cities (current, hourly, daily)
- daily departures and arrivals for german cities (Free Api is restricted to only a few calls a month)

## Technologies:

### Python

- pandas \
  data handling and cleaning with pandas Data Frame
- sqlalchemy\
  connection with RDS and adjustments on Tables
- requests\
  API Calls
- os and dotenv\
  loading and using environment variables for api keys and more
- functools\
  logging tool for data cleaning
- datetime \
  date operations

### MYSQL

- via sqlalchemy\
- MYSQL Workbench\
  checking the status of the RDS. Getting Schema. Adjusting tables

### AWS

- RDS\
  creating cloud based relational Database
- lambda\
  api calls, creation of tables, sending them to the cloud
- event bridge\
  automate lamda functions. Set events for API calls

## License:

MIT License

Copyright (c) 2022 Simo Zilling

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
