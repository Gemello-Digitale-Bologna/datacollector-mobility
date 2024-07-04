import mlrun
import boto3
import os
import datetime

@mlrun.handler()
def share_files(context, bucket: str = "dataspace", path: str = "mobility"):
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
    
    items = {
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-dem/0/dem_model.tif": "image/tiff",
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-osm/0/osm_nodes.parquet": None, 
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-osm/0/osm_edges.parquet": None, 
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-osm/0/osm_buildings.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/merge-osm-dem/0/slope_map.html": "text/html", 
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/merge-osm-dem/0/osm_edges_elevation.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-road-areas/0/ctm_road_areas.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-road-edges/0/rifter_edges.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-road-nodes/0/rifter_nodes.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-curves/0/ctm_level_curves_10m.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-sidewalks/0/ctm_level_sidewalks.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-city30/0/city_30.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-charging-stations/0/charging_stations.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-bike-path/0/bike_path.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-incidents/0/car_incidents.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-bike-parking-places/0/bike_parking_places.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-car-parkings/0/car_parkings.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-bus-stops-tper/0/tper_bus_stops.parquet": None,
        "s3://datalake/projects/mobility-data/artifacts/mobility-data-pipeline/download-train-stops-tper/0/tper_train_stops.parquet": None,
    }
    
    base_folder = './data'
        
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    for p, ct in items.items():
        fname = p.split("/")[-1]
        di = mlrun.get_dataitem(p)
        di.download("./data/" +fname)
        name = fname.split(".")[0]
        s3.upload_file("./data/" +fname, bucket, path + '/' + name + '/' + path_latest + '/' + fname, ExtraArgs={'ContentType': ct if ct else 'application/octet-stream'})
        s3.upload_file("./data/" +fname, bucket, path + '/' + name + '/' + path_date + '/' + fname, ExtraArgs={'ContentType': ct if ct else 'application/octet-stream'})