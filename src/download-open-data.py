
import pandas as pd
import requests
import json as js
import geopandas as gpd
import os
import boto3
import datetime

base_url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/{name}/exports/geojson"

def download_share_geo(project, bucket, name, artifact_name):
    json = requests.get(base_url.format(name = name)).json()  
    if not os.path.exists('./data'):
        os.makedirs('./data')

    path_geojson = './data/'+name+'.geojson'
    with open(path_geojson, 'w') as out_file:
        out_file.write(js.dumps(json, indent=4))
    gdf = gpd.read_file(path_geojson)
    
    path_parquet = './data/'+name+'.parquet'
    gdf.to_parquet(path_parquet)
    share_files(project, bucket, path_parquet, artifact_name)  

def share_files(project, bucket: str = "dataspace", path: str = "city", artifactName: str = 'artifact'):
    """
    Uploads specified data items to a shared S3 bucket and folder.
    Requires the environment variables for S3 endpoint and credentials (S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
    args:
        bucket: The name of the bucket
        path: The path within the bucket
    """

    s3 = boto3.client('s3',
                    endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    
    path_date = datetime.datetime.now().strftime("%Y-%m-%d")
    path_latest = 'latest'
    
    fname = path.split("/")[-1]
    name = fname.split(".")[0]

    print(bucket)
    s3.upload_file("./data/" +fname, bucket, '/' + artifactName + '/' + path_latest + '/' + fname, ExtraArgs={'ContentType': 'application/octet-stream'})
    s3.upload_file("./data/" +fname, bucket, '/' + artifactName + '/' + path_date + '/' + fname, ExtraArgs={'ContentType': 'application/octet-stream'})    

def download_road_areas(project, bucket):
    data = download_share_geo(project, bucket, "aree-stradali", "ctm_road_areas")

def download_curves(project, bucket):
    data = download_share_geo(project, bucket, "carta-tecnica-comunale-curve-livello-10-metri", "ctm_level_curves_10m")

def download_sidewalks(project, bucket):
    data = download_share_geo(project, bucket, "carta-tecnica-comunale-marciapiedi", "ctm_level_sidewalks")

def download_road_edges(project, bucket):
    data = download_share_geo(project, bucket, "rifter_arcstra_li", "rifter_edges")

def download_road_nodes(project, bucket):
    data = download_share_geo(project, bucket, "rifter_nodi_pt", "rifter_nodes")

def download_city_30(project, bucket):
    data = download_share_geo(project, bucket, "velocita-citta-30", "city_30")

def download_charging_stations(project, bucket):
    data = download_share_geo(project, bucket, "colonnine-elettriche", "charging_stations")

def download_bike_path(project, bucket):
    data = download_share_geo(project, bucket, "piste-ciclopedonali", "bike_path")

def download_incidents(project, bucket):
    data = download_share_geo(project, bucket, "incidenti_new", "car_incidents")

def download_bike_parking_places(project, bucket):
    data = download_share_geo(project, bucket, "rastrelliere-per-biciclette", "bike_parking_places")

def download_car_parkings(project, bucket):
    data = download_share_geo(project, bucket, "parcheggi", "car_parkings")

def download_bus_stops_tper(project, bucket):
    data = download_share_geo(project, bucket, "tper-fermate-autobus", "tper_bus_stops")

def download_train_stops_tper(project, bucket):
    data = download_share_geo(project, bucket, "stazioniferroviarie_20210401", "tper_train_stops")
