import mlrun
import pandas as pd
import requests
import json as js
import geopandas as gpd
import os

base_url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/{name}/exports/geojson"

def download_geo(context, name, artifact_name):
    json = requests.get(base_url.format(name = name)).json()  
    if not os.path.exists('./data'):
        os.makedirs('./data')

    with open('./data/'+name+'.geojson', 'w') as out_file:
            out_file.write(js.dumps(json, indent=4))
    gdf = gpd.read_file('./data/'+name+'.geojson')
    
    gdf.to_parquet('./data/'+name+'.parquet')
    with open('./data/'+name+'.parquet', 'rb') as in_file:
        content = in_file.read()
        context.log_artifact(artifact_name, body=content, format="parquet", db_key=artifact_name)
    return json

@mlrun.handler()
def download_road_areas(context):
    data = download_geo(context, "aree-stradali", "ctm_road_areas")
    # return data

@mlrun.handler()
def download_curves(context):
    data = download_geo(context, "carta-tecnica-comunale-curve-livello-10-metri", "ctm_level_curves_10m")
    # return data

@mlrun.handler()
def download_sidewalks(context):
    data = download_geo(context, "carta-tecnica-comunale-marciapiedi", "ctm_level_sidewalks")
    # return data

@mlrun.handler()
def download_road_edges(context):
    data = download_geo(context, "rifter_arcstra_li", "rifter_edges")
    # return data

@mlrun.handler()
def download_road_nodes(context):
    data = download_geo(context, "rifter_nodi_pt", "rifter_nodes")
    # return data

@mlrun.handler()
def download_city_30(context):
    data = download_geo(context, "velocita-citta-30", "city_30")
    # return data

@mlrun.handler()
def download_charging_stations(context):
    data = download_geo(context, "colonnine-elettriche", "charging_stations")
    # return data

@mlrun.handler()
def download_bike_path(context):
    data = download_geo(context, "piste-ciclopedonali", "bike_path")
    # return data

@mlrun.handler()
def download_incidents(context):
    data = download_geo(context, "incidenti_new", "car_incidents")
    # return data

@mlrun.handler()
def download_bike_parking_places(context):
    data = download_geo(context, "rastrelliere-per-biciclette", "bike_parking_places")
    # return data

@mlrun.handler()
def download_car_parkings(context):
    data = download_geo(context, "parcheggi", "car_parkings")
    # return data

@mlrun.handler()
def download_bus_stops_tper(context):
    data = download_geo(context, "tper-fermate-autobus", "tper_bus_stops")
    # return data

@mlrun.handler()
def download_train_stops_tper(context):
    data = download_geo(context, "stazioniferroviarie_20210401", "tper_train_stops")
    # return data

    