# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 17:32:18 2026

@author: rolo

This example illustrates how to download meteorological observations from DMI and compute ETP using the penmon package.
In some cases, meteorological variables are measured in slightly different locations at the same station and then registered 
under different station IDs. This is handled in this example, where the radiation variable is found from a different station.

Do quality check the resulting ETP values. For example, ETP cannot be negative.
"""

import numpy as np
import pandas as pd
from datetime import datetime
from dmi_open_data import DMIOpenDataClient, Parameter
import penmon as pm # https://github.com/sherzodr/penmon


# Set access key 
client = DMIOpenDataClient(api_key='INSERT_YOUR_API_KEY_HERE')

# Choose variables to be imported
variables =[['temp',Parameter.TempDry], # Present air temperature measured 2 m over terrain [°C]
            ['humid',Parameter.Humidity], # Present relative humidity measured 2 m over terrain [%]
            ['wind',Parameter.WindSpeed], # Latest 10 minutes' mean wind speed measured 10 m over terrain [m/s]
            ['radia',Parameter.RadiaGlob], # Latest 10 minutes global radiation mean intensity [W/m²]
            ['precip',Parameter.PrecipPast1h]] #accumulated precipitation over the last hour [mm]

df_hour = pd.DataFrame() 
    
for i in range(len(variables)):
    print(variables[i][0])
    observations = client.get_observations(
                parameter=variables[i][1],
                station_id='06170', # Tylstrup
                from_time=datetime(2025, 1, 1),
                to_time=datetime(2025, 10, 31),
                limit=300000)
    
    if i == 3:
        observations = client.get_observations(
                    parameter=variables[i][1],
                    station_id='06188', # Tylstrup
                    from_time=datetime(2025, 1, 1),
                    to_time=datetime(2025, 10, 31),
                    limit=300000)

    df = pd.DataFrame() 
    df[variables[i][0]] = [x['properties']['value'] for x in observations]
    
    timestamps = [x['properties']['observed'] for x in observations]
    df.index = [datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ') for x in timestamps]
    
    df = df.resample('h').mean()# resample at 1h resolution mean
    # df = df.ffill() # fill gaps with last valid value. as a default you don't want to do this, because you insert wrong observations
    df_hour[variables[i][0]] = df[variables[i][0]]
    
# Compute daily statistics
df_daily_min = df_hour.resample('D').min()
df_daily_mean = df_hour.resample('D').mean()
df_daily_max = df_hour.resample('D').max()

# Compute daily ETo
station = pm.Station(latitude=55.5866692, altitude=12) # Tylstrup DMI station
station.anemometer_height = 10

for date in df_daily_mean.index:
    datetime.strftime(date, '%Y-%m-%d')
    day = station.day_entry(datetime.strftime(date, '%Y-%m-%d'))
    day.temp_min = df_daily_min.at[date, 'temp']
    day.temp_mean = df_daily_mean.at[date, 'temp']
    day.temp_max = df_daily_max.at[date, 'temp']
    day.wind_speed = df_daily_mean.at[date, 'wind']
    day.humidity_mean = df_daily_mean.at[date, 'humid']
    day.radiation_s = df_daily_mean.at[date, 'radia'] * 0.0864 # Convert to MJ/m2/day
    try:
        df_daily_mean.at[date, 'ETp'] = day.eto()
        #last_value = day.eto()
    except:
        df_daily_mean.at[date, 'ETp'] = np.nan


#df_daily_mean.to_csv('../dmi/ETp/daily_mean_ET0.csv', sep=';')
