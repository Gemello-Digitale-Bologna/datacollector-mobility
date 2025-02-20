# import mlrun
import requests
import boto3
import os
import datetime
from datetime import datetime

base_url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/{name}/exports/parquet?lang=it&refine=anno%3A%22{year}%22&timezone=Europe%2FRome"

s3_path = "mobility-data"
path_latest = "latest"

def s3_store(s3_client, s3_bucket, local_file, fname, year, latest):
    s3_client.upload_file(local_file, s3_bucket, s3_path + '/vehicles/' + str(year) + '/' + fname,
                          ExtraArgs={'ContentType':'application/octet-stream'})
    if latest:
        s3_client.upload_file(local_file, s3_bucket, s3_path + '/vehicles/' + path_latest + '/' + fname,
                          ExtraArgs={'ContentType':'application/octet-stream'})
                

        
def write_year(project, s3_bucket, s3_client, year, name, parquet_file, latest):    
    s3_store(s3_client, s3_bucket, parquet_file, name+'.parquet', year, latest)
    project.log_artifact(name=name, kind="artifact", source=parquet_file)
    
    print(f"write vehicles data year {year}")
    
    
def download_vehicles_by_year(project, bucket, year):
    s3 = boto3.client('s3',
                    endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    
    if not os.path.exists('./data'):
        os.makedirs('./data')

    if not os.path.exists('./data/'+str(year)):
        os.makedirs('./data/'+str(year))

    name = "parco-circolante-veicoli"
    parquet_file = './data/'+str(year)+'/'+name+'.parquet'

    #print(f"get link {base_url.format(name = name, year = year)}")
    response = requests.get(base_url.format(name = name, year = year))
    print(f"get response {response.status_code}")
    
    if response.status_code == 200:
        with open(parquet_file, 'bw') as out_file:
            out_file.write(response.content)
    
    write_year(project, bucket, s3, year, name, parquet_file, False)

