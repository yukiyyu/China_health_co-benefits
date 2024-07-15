# -*- coding: utf-8 -*-
"""
Created on Tue Oct 18 15:07:56 2022

@author: huang


"""

import pandas as pd
import os
import numpy as np
from sklearn import preprocessing

# In[]

file = r'StationList-2020.01.01.csv'

data = pd.read_csv(file,encoding='utf-8')

city = data['城市'].unique()

city = pd.DataFrame(data = city)

writer = pd.ExcelWriter(r'city.xlsx')

city.to_excel(writer)

writer.close()


# In[]
# The data for 2019, 2020, and 2021 need to be processed in three separate folders and then merged into one file. Here we take 2019 as an example.
for root_dir, sub_dir, files in os.walk(r'Stations_20190101-20191231\20190101-20191231'):
#   print(root_dir, sub_dir, files)
    date = list(map(str, pd.date_range(start="20190101", end="20191231", freq="D").tolist()))
    for i in range(0, len(date)):
#       print(d[0:4]+d[5:7]+d[8:10]) 20191231
        dt = date[i]
        dt = dt[0:4] + dt[5:7] + dt[8:10]
        root = r'Stations_20190101-20191231\20190101-20191231\china_sites_'
        filepath = root + dt + '.csv'
        data = pd.read_csv(filepath, encoding='utf-8')
        
        PM25_0 = data[data['type'] == 'PM2.5']
#       size=PM25.shape # 24*1608, columns 2-1606 are station data
        PM25 = pd.DataFrame(preprocessing.scale(PM25_0.iloc[:, 3:]), columns=PM25_0.columns[3:])
        city_z = np.mean(PM25, axis=1)
        
        # Get the previous day's data
        if i > 0:
            dp = date[i-1]
            dp = dp[0:4] + dp[5:7] + dp[8:10]
            file = root + dp + '.csv'
            data_p = pd.read_csv(file, encoding='utf-8')
            PM25_p = data_p[data_p['type'] == 'PM2.5']
            PM25_p = pd.DataFrame(preprocessing.scale(PM25_p.iloc[:, 3:]))
            city_z_p = np.mean(PM25_p, axis=1)
        else:
            PM25_p = []
        
        # Calculate z-score
        for c in range(0, len(PM25.columns) - 1):
            station_data = PM25.iloc[:, c]
            std = np.std(station_data)
            mean = np.mean(station_data)
            dropflag = False
            
            # 1. Check if the station data for the day has less than 12 hours of data
            if len(station_data[np.isnan(station_data)]) >= 12:
                print(PM25.columns[c], 'True')
                dropflag = True
                
            # 2. Check if z-score > 4
            if abs(station_data.all()) > 4:
                dropflag = True
                
            # 3. Check the z-score difference between two consecutive days
            # 4. Check the z-score
            if len(PM25_p) != 0:
                station_data_p = PM25_p.iloc[:, c]
                if (station_data - station_data_p).any() > 6:
                    dropflag = True
                if ((station_data - station_data_p) / (city_z - city_z_p)).any() > 2:
                    dropflag = True
            if dropflag == True:
                print(PM25_0.columns[c + 3])
                PM25 = PM25.drop(labels=PM25.columns[c], axis=1)
        
        print(PM25.shape)

