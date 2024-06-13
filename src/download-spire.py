import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import mlrun
import json
import boto3

import pandas as pd
import datetime

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def getGService(context):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    
    token_info_var = context.get_secret('GOOGLE_TOKEN') or os.environ.get('GOOGLE_TOKEN')

    if token_info_var is not None:
        token_info = json.loads(token_info_var)
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    else:
        raise Exception("No valid credentials")

    try:
        service = build("drive", "v3", credentials=creds)

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
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

def process_file(service, item_id, item_name):
    r = service.files().get_media(fileId=item_id)
    local_path = 'myfile'
    with open(local_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, r)
        done = False
        while not done:
            status, done = downloader.next_chunk()

    try:
        data = datetime.datetime.strptime(item_name, 'FLUSSI%Y%m%d.txt')
    except Exception as e:
        print(f"Error: unknown file : {item_name}")
        return
    
    f = open(f"myfile", "r")
    sensor = None
    entries = []
    for x in f:
        if x.startswith('Section'):
            sensor = x.split('Section ')[1].strip()
        else:
            arr = x.split('\t')
            for i in range(len(arr)):
                if arr[i].strip() != '':
                    d = data.strftime("%Y-%m-%d")
                    t = datetime.time(hour = i // 12, minute = i % 12 * 5).strftime("%H:%M")
                    entries.append({
                        'sensor_id': sensor,
                        'date': d,
                        'time': t,
                        'start': d + ' ' + t,
                        'value': int(int(arr[i]) / 12)
                    })

    df = pd.DataFrame(entries)
    fname = data.strftime("%Y-%m")
    if not os.path.exists(f"data/{fname}.parquet"):
        df.to_parquet(f"data/{fname}.parquet")
    else:
        rdf = pd.read_parquet(f"data/{fname}.parquet")
        rdf = pd.concat([rdf, df])
        rdf.to_parquet(f"data/{fname}.parquet")
    
def process_folder(service, folder):
    """Downloads recursively all content from a specific year folder on Google Drive."""
    files = service.files()
    request = files.list(q=f"'{folder['id']}' in parents", 
                         supportsAllDrives=True, includeItemsFromAllDrives=True, 
                         fields="nextPageToken, files(id, name, mimeType)")
    print('folder', folder['name'])
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
                process_folder(service, item)
            else:
                # print(f"Downloading file {item_name}...")
                process_file(service, item_id, item_name)
        request = files.list_next(request, results)

def process_all(context, query: str, s3, bucket: str, destination_path: str):
    service = getGService(context)
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
                    
                process_folder(service, year)
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
            
@mlrun.handler()
def get_spire(context):
    base_folder = './data'
        
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))


    process_all(context, "mimeType='application/vnd.google-apps.folder' and name='Flussi spire'", s3, "dataspace", "mobility-data/trafic-spire")
