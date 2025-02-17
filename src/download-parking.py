
import pandas as pd
import geopandas as gpd
import requests
import json as js
import boto3
import os

base_url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/{name}/exports/geojson?lang=it&timezone=UTC"

s3_path = "mobility-data"
path_latest = "latest"

def s3_store(s3_client, s3_bucket, local_file, fname, year, latest):
    s3_client.upload_file(local_file, s3_bucket, s3_path + '/parking-availability/' + str(year) + '/' + fname,
                          ExtraArgs={'ContentType':'application/octet-stream'})
    if latest:
        s3_client.upload_file(local_file, s3_bucket, s3_path + '/parking-availability/' + path_latest + '/' + fname,
                              ExtraArgs={'ContentType':'application/octet-stream'})
        

def s3_read(s3_client, s3_bucket, local_file, fname, year):
    try:
        s3_client.download_file(s3_bucket, s3_path + '/parking-availability/' + str(year) + '/' + fname, local_file)
    except:
        print(f"file not exists on S3 {year} {fname}")
       
    
def write_year(project, s3_bucket, s3_client, actual_df, year, fname, latest):
    #filter by year
    rslt_df = actual_df[actual_df['anno'] == year]
    
    #check existing dataset
    if not os.path.exists('./data/'+str(year)):
        os.makedirs('./data/'+str(year))

    parquet_file = './data/'+str(year)+'/'+fname+'.parquet'
    s3_read(s3_client, s3_bucket, parquet_file, fname+'.parquet', year)
    if os.path.exists(parquet_file):
        old_df = gpd.read_parquet(parquet_file)
        new_df = pd.concat([old_df, rslt_df], ignore_index=True).drop_duplicates(subset=['guid', 'data'])
        new_df.to_parquet(parquet_file)
    else:
        rslt_df.to_parquet(parquet_file)

    s3_store(s3_client, s3_bucket, parquet_file, fname+'.parquet', year, latest)

    artifact_name = str(year) + '-' + fname
    project.log_artifact(name=artifact_name, kind="artifact", source=parquet_file)   
    
    
def download_parking_availability(project, bucket):
    s3 = boto3.client('s3',
                    endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    
    name = "disponibilita-parcheggi-storico"
    json = requests.get(base_url.format(name = name)).json()
    
    if not os.path.exists('./data'):
        os.makedirs('./data')

    with open('./data/'+name+'.geojson', 'w') as out_file:
        out_file.write(js.dumps(json, indent=2))
    
    gdf = gpd.read_file('./data/'+name+'.geojson')
    gdf['anno'] = gdf['data'].dt.year
       
    years = gdf['anno'].unique()
    for index, year in enumerate(years):
        write_year(project, bucket, s3, gdf, year, name, (index==(len(years)-1)))
