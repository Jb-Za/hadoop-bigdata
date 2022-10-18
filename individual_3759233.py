from pyspark.sql import *
import time
import datetime as dt
from datetime import timedelta
import pyspark
import pandas as pd
import calendar
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,array_contains
import folium 
import json
from dateutil import tz
import requests
from dateutil import parser

nowTime = dt.datetime.now()
curDay = calendar.day_name[nowTime.weekday()]
curHour = nowTime.hour
print(curDay)
print(curHour)

columnnn = 'hours_{}'.format(curDay)
print(columnnn)

######################################## flight api stuff ########################################

params = { # flights from heathrow to philadelphia
    'arr_iata' : 'PHL',
    'dep_iata' : 'LHR'
}
method = 'flights'
api_base = 'https://airlabs.co/api/v9/schedules?api_key=$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$' #api key, i suggest getting your own
api_result = requests.get(api_base, params)
api_response = api_result.json()

#print(api_response)


#with open('data.json', 'w') as f:
#   json.dump(api_response, f)

lowest_time = parser.parse("2999-12-30 23:59") # if it prints out this date, there are no more flights left today
for line in api_response['response']: # calculate earliest flight time 
    json_time = parser.parse(line['arr_time'])
    if lowest_time > json_time:
        lowest_time = json_time


######################################## flight api stuff ########################################


######################################## pyspark filtering #######################################

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

data = spark.read.csv('hdfs://kloniphani:9000/user/data/business_data.csv', header = True)


data = data.withColumnRenamed("hours.Friday",   "hours_Friday")
data = data.withColumnRenamed("hours.Saturday", "hours_Saturday")
data = data.withColumnRenamed("hours.Monday",   "hours_Monday")
data = data.withColumnRenamed("hours.Sunday",   "hours_Sunday")
data = data.withColumnRenamed("hours.Thursday", "hours_Thursday")
data = data.withColumnRenamed("hours.Tuesday",  "hours_Tuesday")
data = data.withColumnRenamed("hours.Wednesday","hours_Wednesday")

print('Earliest flight from heathrow lands at {}'.format(lowest_time))
print('Filtering by shop opening times at {}'.format(lowest_time + timedelta(hours=2)))

data = data.filter((col('stars') == 4.0) & ((col('city') == 'Philadelphia')) & col('categories').like("%Restaurants%") )
#data = data.filter(col('hours_{}'.format(curDay)).like("{}:0-%".format(lowest_time.hour + 2))) # this alwaysS doesn't give... nice times for the purposes of  
data = data.filter(col('hours_{}'.format(curDay)).like("8:0-%")) # showcasing this, so i took the liberty of filtering by 8 am.

lat = data.select(col('latitude'))
lon = data.select(col('longitude'))

#data.toPandas().to_csv('test55.csv')

lat = lat.toPandas()
lon = lon.toPandas()
coordinates = pd.concat([lat,lon], axis = 1)

######################################## pyspark filtering #######################################


######################################## tomtom map stuff ######################################


api_key = "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$" #api key, i suggest getting your own
airport_location = [39.8729299,-75.2436673]
def start_map(api_key = api_key,location = airport_location,zoom = 12,style = "main"):
    maps_url = "http://{s}.api.tomtom.com/map/1/tile/basic/"+style+"/{z}/{x}/{y}.png?tileSize=512&key="
    the_map = folium.Map(
        location = location,
        zoom_start = zoom,
        tiles = str(maps_url + api_key),
        attr = "MyMap" 
        )
    return the_map

TomTom_map = start_map()


folium.TileLayer(
tiles = "https://api.tomtom.com/traffic/map/4/tile/flow/relative0/{z}/{x}/{y}.png?tileSize=512&key=" + api_key,
zoom_start = 12,
attr="Traffic",
).add_to(TomTom_map)
TomTom_map.save("tomtom.html")


for index, row in coordinates.iterrows(): # placing the markers on the map
    folium.Marker([row['latitude'] , row['longitude']]).add_to(TomTom_map)
    if index >=60:
        break

TomTom_map.save("tomtom.html")

################################## Routing ####################################
def batch_json(order): # need to create a json object for the api POST
    global coordinates # this function takes in the ordered locations from the waypoint call... [0, 7 , 3 .....]
    with open('sync_batch.json','r+') as file: # i created a... dummy json to make this coding less messy
        updating_json = json.loads(file.read()) # can just append to that object basically.
    
    for index in order: # the initial location needs to be the airport,
        if index == 0: # so we don't need index 0, we just need the next index... index + 1
            next_row = coordinates.iloc[[order[index + 1]]]
            initial_stuff = { "query": "/calculateRoute/{},{}:{},{}/json?travelMode=car&routeType=fastest&traffic=true&departAt=now&maxAlternatives=0".format(39.875852, -75.246227,float(next_row['latitude']),float(next_row['longitude']))}
            updating_json['batchItems'].append(initial_stuff) # push the data into the json object
        else:
            if index + 1 <= 11:
                row = coordinates.iloc[[order[index+ 1]]] # same as above really... take current index and next one...
                next_row = coordinates.iloc[[order[index+ 1] + 1]]
                stuff = { "query": "/calculateRoute/{},{}:{},{}/json?travelMode=car&routeType=fastest&traffic=true&departAt=now&maxAlternatives=0".format(float(row['latitude']), float(row['longitude']),float(next_row['latitude']),float(next_row['longitude']))}
                updating_json['batchItems'].append(stuff)
                coordinates = coordinates.drop(order[index+1], axis = 0)  # removing the rows that have already been used. this allows me to 
                coordinates.index = range(len(coordinates)) # run multiple trucks. 
        #print(coordinates)

    with open("updated_json.json", "w") as outfile:
        outfile.write(json.dumps(updating_json, indent = 4) ) # making it write jsons so i can see them for debugging..
    return updating_json

def waypoint_json(): # this function takes in 12 gps coordinates and returns the json file required to optmize the routes between them. 
    with open('waypoint.json','r+') as file: # the api call is made below which returns their order - [0 , 7 ,3 ... etc] 
        updating_json = json.loads(file.read())
    for index, row in coordinates.iterrows():
        stuff = {"point": {"latitude" : float(row['latitude']) , "longitude" : float(row['longitude'])} }
        updating_json['waypoints'].append(stuff)
        if index >= 10:
            break
    with open("updated_waypoint.json", "w") as outfile:
        outfile.write(json.dumps(updating_json, indent = 4) )
    return updating_json

for iteration in range(5): # 5 trucks... 5 iterations
    updating_json = waypoint_json()
    waypoint_url = "https://api.tomtom.com/routing/waypointoptimization/1/best?key={}".format(api_key)
    response = requests.post(waypoint_url,json=updating_json) # returns the optmized route order

    print(response.status_code)
    if response.status_code == 200:
        response = response.json()

        order = response['optimizedOrder']
        print(order)   
        with open("waypoint_routes.json", "w") as outfile:
            outfile.write(json.dumps(response, indent = 4) )
        
        updated_batch_json = batch_json(order)
        batch_url = "https://api.tomtom.com/routing/1/batch/sync/json?key={}&routeType=fastest&travelMode=car".format(api_key)
        route_response = requests.post(batch_url,json=updated_batch_json) # returns the routes between all of the points
        print(route_response.status_code)
        if route_response.status_code == 200:
            route_response = route_response.json()
            
            #print(response['batchItems'][0]['response']['routes'][0]['legs'][0]['points'])
            #with open("routes.json", "w") as outfile:
            #   outfile.write(json.dumps(response, indent = 4) )
            
            #delay = response['batchItems'][i]['response']['routes'][0]["summary"]['trafficDelayInSeconds']
            #travel_time = response['batchItems'][i]['response']['routes'][0]["summary"]['travelTimeInSeconds']
            for i in range(11):
                points = route_response['batchItems'][i]['response']['routes'][0]['legs'][0]['points']
                route_points = [[point['latitude'], point['longitude']] for point in points]
                #print(route_points)
                if iteration == 0:
                    folium.PolyLine(route_points, color="blue", weight=4, opacity=1).add_to(TomTom_map)
                if iteration == 1:
                    folium.PolyLine(route_points, color="red", weight=4, opacity=1).add_to(TomTom_map)
                if iteration == 2:
                    folium.PolyLine(route_points, color="purple", weight=4, opacity=1).add_to(TomTom_map)
                if iteration == 3:
                    folium.PolyLine(route_points, color="pink", weight=4, opacity=1).add_to(TomTom_map)
                if iteration == 4:
                    folium.PolyLine(route_points, color="black", weight=4, opacity=1).add_to(TomTom_map)       
            with open("batch_routes.json", "w") as outfile:
                outfile.write(json.dumps(route_response, indent = 4) )

    TomTom_map.save("tomtom.html")

######################################## tomtom map stuff ######################################


'''
def matrix_json(): # i found matrix calls to be... useless but its json creator is there incase i need it.
    with open('sync_matrix.json','r+') as file: # good for analysis...
        updating_json = json.loads(file.read())

    for index, row in coordinates.iterrows():
        stuff = { "point": {"latitude" : float(row['latitude']) , "longitude" : float(row['longitude'])} }
        updating_json['destinations'].append(stuff)
        if index >= 14:
            break
    
    with open("updated_matrix_json.json", "w") as outfile:
        outfile.write(json.dumps(updating_json, indent = 4) )
    return updating_json
'''
