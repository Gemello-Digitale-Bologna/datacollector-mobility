import requests
import boto3
import os
import datetime
from datetime import datetime

base_url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/{name}/exports/parquet?lang=it&refine=data%3A%22{year}%22&timezone=Europe%2FRome"

s3_path = "mobility-data"
path_latest = "latest"

def s3_store(s3_client, s3_bucket, local_file, fname, year, latest):
    s3_client.upload_file(local_file, s3_bucket, s3_path + '/bike-flow/' + str(year) + '/' + fname,
                          ExtraArgs={'ContentType':'application/octet-stream'})
    if latest:
        s3_client.upload_file(local_file, s3_bucket, s3_path + '/bike-flow/' + path_latest + '/' + fname,
                          ExtraArgs={'ContentType':'application/octet-stream'})
        
        

        
def write_year(bucket, s3_client, year, name, parquet_file, latest):    
    s3_store(s3_client, bucket, parquet_file, name+'.parquet', year, latest)
    print(f"write bike data year {year}")
    
def download_bike_flow_by_year(project, bucket, year):
    s3 = boto3.client('s3',
                    endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    
    if not os.path.exists('./data'):
        os.makedirs('./data')

    if not os.path.exists('./data/'+str(year)):
        os.makedirs('./data/'+str(year))

    name = "colonnine-conta-bici"
    parquet_file = './data/'+str(year)+'/'+name+'.parquet'

    print(base_url.format(name = name, year = year))
    response = requests.get(base_url.format(name = name, year = year))
    
    if response.status_code == 200:
        with open(parquet_file, 'bw') as out_file:
            out_file.write(response.content)
    
        write_year(context, s3, year, name, parquet_file, False)
        
def download_bike_flow_latest(project, bucket, run):
    year = datetime.now().year
    s3 = boto3.client('s3',
                    endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    
    if not os.path.exists('./data'):
        os.makedirs('./data')

    if not os.path.exists('./data/'+str(year)):
        os.makedirs('./data/'+str(year))

    name = "colonnine-conta-bici"
    parquet_file = './data/'+str(year)+'/'+name+'.parquet'

    print(base_url.format(name = name, year = year))
    response = requests.get(base_url.format(name = name, year = year))
    
    if response.status_code == 200:
        with open(parquet_file, 'bw') as out_file:
            out_file.write(response.content)
    
        write_year(bucket, s3, year, name, parquet_file, True)
        

    project.log_artifact(name=name, kind="artifact", source=parquet_file)
    run.log_metric('epoch', 1)
    