#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by Dr. W SUN on 11/07/2020

import datetime as dt
import time
from datetime import datetime as ddt;

import numpy as np
import pandas as pd

print('{}: START'.format(ddt.now().strftime('%d%b%Y %H:%M:%S:')))
date_start, date_end = '2015-01-01', ddt.now().strftime('%Y-%m-%d')

WeatherStations = pd.read_csv('WeatherStations.csv')
list_stations = WeatherStations['station']
list_cities = WeatherStations['city']

# weather: stations, from_ and to_dates - get previous day too
# Weather History
date_yest = (ddt.strptime(date_start, '%Y-%m-%d') - dt.timedelta(days=1)).strftime('%Y-%m-%d')
year1, month1, day1 = ddt.strptime(date_yest, '%Y-%m-%d').year, ddt.strptime(date_yest, '%Y-%m-%d').month, ddt.strptime(
    date_yest, '%Y-%m-%d').day
year2, month2, day2 = ddt.strptime(date_end, '%Y-%m-%d').year, ddt.strptime(date_end, '%Y-%m-%d').month, ddt.strptime(
    date_end, '%Y-%m-%d').day

df_weather_Hist = pd.DataFrame();


def get_raw_weatherdata():
    for idcity in list_stations:
        print('{}: Downloading weather for {}'.format(ddt.now().strftime('%d%b%Y %H:%M:%S:'), idcity))
        API_weather = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&data=tmpc&year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc%2FUTC&format=onlycomma&latlon=yes&direct=yes&report_type=1&report_type=2'.format(
            idcity, year1, month1, day1, year2, month2, day2)
        df_tmp_weather = pd.read_csv(API_weather, sep=',', header=0)
        df_weather_Hist = df_weather_Hist.append(df_tmp_weather)
        time.sleep(60)
    return df_weather_Hist


def process_data(df_weather_Hist, population_weighted_average=True):
    # format columns; clean temp column from any non-numeric data
    df_weather_Hist = pd.merge(df_weather_Hist, WeatherStations, how='left', on='station')
    df_weather_Hist['valid'] = pd.to_datetime(df_weather_Hist['valid'])
    df_weather_Hist['tmpc'] = pd.to_numeric(df_weather_Hist['tmpc'], errors='coerce')
    df_weather_Hist.dropna(inplace=True)

    if population_weighted_average:
        # gaverage according to their population weight
        df_weather_Hist_avg = df_weather_Hist.groupby(df_weather_Hist.valid).apply(
                lambda x: np.average(x['tmpc'], weights=x.population))
    else:
        # simple daily average
        df_weather_Hist_avg = df_weather_Hist[['valid', 'tmpc']].groupby('valid').mean()
    df_weather_Hist_avg.name = 'avg_tmpc'
    df_weather_Hist_avg = df_weather_Hist_avg.reset_index()
    df_weather_Hist_avg['datetime'] = df_weather_Hist_avg['valid'].dt.round('30min')
    del df_weather_Hist_avg['valid']
    df_weather_Hist_avg['date'], df_weather_Hist_avg['time'] = df_weather_Hist_avg['datetime'].dt.date, \
                                                               df_weather_Hist_avg['datetime'].dt.time
    return df_weather_Hist_avg


if __name__ == '__main__':
    df_weather_Hist = get_raw_weatherdata()
    df_weather_Hist = process_data(df_weather_Hist)
    df_weather_Hist = df_weather_Hist.round({'avg_tmpc': 3})
    df_weather_Hist[['datetime', 'avg_tmpc', 'date', 'time']].to_csv('national_weather_2015_2020')
