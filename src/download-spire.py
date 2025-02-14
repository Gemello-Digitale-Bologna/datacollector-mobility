
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

import json
import boto3

import pandas as pd
import datetime

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def getGService(project, token_uri):
    creds = None
    token = token_uri.as_file()
    try:
        token_info = json.load(open(token))
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        service = build("drive", "v3", credentials=creds)
    except HttpError as error:
        print(f"An error occurred: {error}")

    return service

def upload_file(s3, bucket: str, path: str, local_path: str, item_name: str):
    """
    Uploads specified data items to a shared S3 bucket and folder.
    Requires the environment variables for S3 endpoint and credentials (S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
    args:
        bucket: The name of the bucket
        path: The path within the bucket
    """

    name = path + '/' + item_name
    s3.upload_file(local_path, bucket, name, ExtraArgs={'ContentType': 'application/octet-stream'})


def extract_date_flussi(item_name):
    try:
        return datetime.datetime.strptime(item_name, 'FLUSSI%Y%m%d.txt')
    except Exception as e:
        try:
            return datetime.datetime.strptime(item_name, 'FLUSS%Y%m%d.txt')
        except Exception as e2:
            return None

def extract_date_accuracy(item_name):
    try:
        return datetime.datetime.strptime(item_name, 'accur%Y%m%d.txt')
    except Exception as e:
        try:
            return datetime.datetime.strptime(item_name, 'accur%Y%m%d.txt')
        except Exception as e2:
            return None
       
def process_file(service, item_id, item_name, date_extractor):
    r = service.files().get_media(fileId=item_id)
    local_path = 'myfile'
    with open(local_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, r)
        done = False
        while not done:
            status, done = downloader.next_chunk()

    data = date_extractor(item_name)
    if data == None:
        print(f"Error: unknown file : {item_name}")
        return
    
    f = open(f"myfile", "r")
    sensor = None
    entries = []
    count = 0
    for x in f:
        if x.startswith('Section'):
            sensor = x.split('Section ')[1].strip()
            count = 0
        else:
            arr = x.split('\t')
            lc = 0
            for i in range(len(arr)):
                if arr[i].strip() != '':
                    d = data.strftime("%Y-%m-%d")
                    t = datetime.time(hour = (count + i) // 12, minute = i % 12 * 5).strftime("%H:%M")
                    entries.append({
                        'sensor_id': sensor,
                        'date': d,
                        'time': t,
                        'start': d + ' ' + t,
                        'value': int(int(arr[i]) / 12)
                    })
                    lc += 1
            count += lc

    df = pd.DataFrame(entries)
    fname = data.strftime("%Y-%m")
    if not os.path.exists(f"data/{fname}.parquet"):
        df.to_parquet(f"data/{fname}.parquet")
    else:
        rdf = pd.read_parquet(f"data/{fname}.parquet")
        rdf = pd.concat([rdf, df])
        rdf.to_parquet(f"data/{fname}.parquet")
    
def process_folder(service, folder, date_extractor):
    """Downloads recursively all content from a specific year folder on Google Drive."""
    files = service.files()
    request = files.list(q=f"'{folder['id']}' in parents", 
                         supportsAllDrives=True, includeItemsFromAllDrives=True, 
                         fields="nextPageToken, files(id, name, mimeType)")
    print(f"folder {str(folder['name'])}")
    while request is not None:
        results = request.execute()
        
        items = results.get("files", [])
        for item in items:
            item_name = item["name"]
            item_id = item["id"]
            item_type = item["mimeType"]

            # If it's a folder, recursively download its content as it is month folder
            if item_type == "application/vnd.google-apps.folder":
                print(f"Downloading folder {item_name}...")
                process_folder(service, item, date_extractor)
            else:
                print(f"Downloading file {item_name}...")
                process_file(service, item_id, item_name, date_extractor)
        request = files.list_next(request, results)

def process_all(project, token_uri, query: str, s3, bucket: str, destination_path: str, date_extractor):
    service = getGService(project, token_uri)
    results = (service.files()
               .list(q=query, pageSize=1, fields="files(id, name, mimeType)", supportsAllDrives=True, includeItemsFromAllDrives=True)
               .execute())
    
    root_items = results.get("files", [])
    for item in root_items:
        files = service.files()
        request = files.list(q=f"'{item['id']}' in parents", 
                     supportsAllDrives=True, includeItemsFromAllDrives=True, 
                     fields="nextPageToken, files(id, name, mimeType)")
        while request is not None:
            results = request.execute()
            years = results.get("files", [])
            for year in years:
                # if year["name"] == "2024":
                #     continue
                    
                process_folder(service, year, date_extractor)
                rdf = pd.DataFrame()
                for i in range(1, 13):
                    mf = datetime.date(int(year["name"]), i, 1).strftime("%Y-%m")
                    if os.path.exists(f"data/{mf}.parquet"):
                        rdf = pd.concat([rdf, pd.read_parquet(f"data/{mf}.parquet")])
                rdf.to_parquet(f"data/{year['name']}.parquet")
                upload_file(s3, bucket, destination_path + "/" + year["name"], f"data/{year['name']}.parquet", "trafic-spire.parquet")
                if year["name"] == datetime.datetime.now().strftime("%Y"):
                    upload_file(s3, bucket, destination_path + "/latest", f"data/{year['name']}.parquet", "trafic-spire.parquet")
                    
            request = files.list_next(request, results)

def get_spire(project, token_uri, bucket):
    
    base_folder = './data'
        
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))


    process_all(project, token_uri, "mimeType='application/vnd.google-apps.folder' and name='Flussi spire'", s3, bucket, "mobility-data/trafic-spire", extract_date_flussi)


def get_spire_accur(project, token_uri, bucket):
    base_folder = './data'
        
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))


    process_all(project, token_uri, "mimeType='application/vnd.google-apps.folder' and name='Diagnostica'", s3, bucket, "mobility-data/trafic-spire-accur", extract_date_accuracy)
