import pandas as pd
import numpy as np
import haversine as hs 

data = pd.read_csv('seoul_accident_drop.csv', encoding='cp949')
link_raw_data = pd.read_csv('link_seoul_ver5.csv', encoding='cp949')
dataset = pd.DataFrame(columns=['datetime', 'lanes', 'limit', 'length', 
    'bump', 'camera', 'CONV', 'KINDER', 'TRAVEL','SCHOOL','BROKER', 'rain', 'temp', 'humid', 'visibility', 'dew_point', 'cloud', 'vaper_press', 'ground_temp', 'sun_diff_angle', 'sun_elevation'])

dataset['datetime'] = data['발생일시']
latitude = data['위도']
longitude = data['경도']
lat_lon = []
for i in range(len(latitude)):
    lat_lon.append([longitude[i], latitude[i]])
    
df = pd.read_csv('link_seoul_ver3.csv', encoding='cp949')
link = df['LINK_ID'].values
geometry = df['geometry']

geometry = geometry.str.replace('LINESTRING','')
geometry = geometry.str.replace('(','')
geometry = geometry.str.replace(')','')
geometry = geometry.str.replace(' ',',')
geometry = geometry.str.replace(',,',',')
geometry = geometry.str.split(',')
for i in range(len(geometry)):
    geometry[i].pop(0)

def distance(x1,y1,x2,y2,x3,y3):
    if x1 == x2:
        m = 0
    else:
        m = (y2-y1)/(x2-x1)
    a = m
    b = -1
    c = y1 - m*x1
    d = abs(a*x3 + b*y3 + c) / np.sqrt(a**2 + b**2)
    return d

def distance_link(link, geometry, x, y):
    d = []
    for i in range(0, len(geometry[link]) - 2, 2):
        x1 = float(geometry[link][i+1])
        y1 = float(geometry[link][i])
        x2 = float(geometry[link][i+3])
        y2 = float(geometry[link][i+2])
        d.append(distance(x1,y1,x2,y2,x,y))
    return min(d)

def distance_haversine(link, geometry, x, y):
    for i in range(len(geometry[link])):
        if i % 2 == 0:
            x1 = float(geometry[link][i+1])
            y1 = float(geometry[link][i])
            if hs.haversine((x,y),(x1,y1)) <= 0.3:
                return True
            else:
                continue
        else:
            continue
    

def link_haversine(geometry, x, y):
    link_haversine = []
    for i in range(len(geometry)):
        if distance_haversine(i,geometry,x,y) == True:
            link_haversine.append(i)
    return link_haversine

def link_shortest(link_haversine, geometry, x, y):
    d = []
    for i in range(len(link_haversine)):
        d.append(distance_link(link_haversine[i],geometry,x,y))
    return link_haversine[d.index(min(d))]


def mapping_link(x,y):
    linked_haversine = link_haversine(geometry, x, y)
    return link_shortest(linked_haversine, geometry, x, y)

link_id = []
for i in range(25000, len(lat_lon)):
    link_result = mapping_link(lat_lon[i][0], lat_lon[i][1])
    link_data = link_raw_data.loc[link_result]
    link_id.append(link_data['LINK_ID'])
    if i % 100 == 0:
        print(i, '번째 완료')

link_id = pd.DataFrame(link_id)
link_id.to_csv('link_id.csv', encoding='cp949')
