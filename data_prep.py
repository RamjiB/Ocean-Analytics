import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import netCDF4 as nc
# import multiprocessing


# from atpbar import atpbar, find_reporter, flush, register_reporter
from tqdm import tqdm_notebook, tqdm
# import matplotlib.pyplot as plt
import zipfile
# from shapely.geometry.polygon import Polygon
# from shapely.geometry import Point

def sst_calculation(traffic_data):
    for i in traffic_data.index:
        x = np.where((lat == traffic_data['LAT_CL'][i]))[0][0]
        y = np.where((lon == traffic_data['LON_CL'][i]))[0][0]
        SST = sst[x-1:x+1,y-1:y+1]
        traffic_data.loc[i, 'SST'] = SST[SST!=-32768.].mean()
    traffic_data = traffic_data[~traffic_data['SST'].isna()]
    return traffic_data


files = sorted(os.listdir('sat_data/whole_data/'))
days = 60
day_count = 0
for file in tqdm(files):
    # if file[:4] == '2021':
        # day_count += 1
        # if day_count > days:
        #     break
        # elif day_count > 20:
        #     # pass
        #     print(file, day_count)
        # processing sat data
    fn = os.path.join('sat_data/whole_data/', file)
    ds = nc.Dataset(fn)
    lat = np.array(ds.variables['lat'][:])
    lon = np.array(ds.variables['lon'][:])
    sst = np.array(ds.variables['analysed_sst'][:]).squeeze()
    date = nc.num2date(ds.variables["time"],
                       units=ds.variables['time'].getncattr('units'))[:][0].strftime()

    print(date, lat.min(), lat.max(), lon.min(), lon.max())

    #     print('Reading Traffic Data')

    fname = 'AIS_' + date.split(' ')[0].replace('-', '_') + '.zip'
    file_name = zipfile.ZipFile(os.path.join('traffic_Data/whole_data/', fname), 'r')
    file_name.extractall('traffic_Data/')
    filename = os.path.join('traffic_Data/', fname.split('.')[0] + '.csv')
    traffic_data = pd.read_csv(filename)
    os.remove(filename)
    #     print('processing traffic data')
    traffic_data = traffic_data[(traffic_data['LON'] > -123.92578125) & (traffic_data['LON'] < -113.90625) &
                                (traffic_data['LAT'] < 37.3002752813443) & (traffic_data['LAT'] > 31.203404950917395)]
    traffic_data = traffic_data.iloc[:int(len(traffic_data)*0.05), :]
    # print(traffic_data.shape)
    traffic_data['LAT_CL'] = traffic_data['LAT'].apply(lambda x: min(lat, key=lambda y: abs(y - x)))
    # print('LAT Done')
    traffic_data['LON_CL'] = traffic_data['LON'].apply(lambda x: min(lon, key=lambda y: abs(y - x)))
    # print('LON Done')
    traffic_data['LAT_CL'] = traffic_data['LAT_CL'].round(3)
    traffic_data['LON_CL'] = traffic_data['LON_CL'].round(3)

    traffic_data = sst_calculation(traffic_data)
    # traffic_data = traffic_data.reset_index(drop='index', inplace=True)


    traffic_data.to_csv('analysis_data/' + date.split(' ')[0].replace('-', '_') + '.csv', index=False, compression='xz')
