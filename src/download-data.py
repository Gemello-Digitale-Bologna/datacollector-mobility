import mlrun
import pickle
import osmnx as ox
import os
import numpy as np
import geopandas as gpd
from sklearn.neighbors import NearestNeighbors
import folium
import rasterio
import xarray as xr
import rioxarray 
import warnings
from urllib3.exceptions import InsecureRequestWarning
from owslib.util import Authentication
from pyproj import Transformer
import requests
import json

base_folder = './data'

@mlrun.handler()
def download_dem(context, query: str):
    """
    Download the DEM (digital elevation model) data for the city/region involved.
    args:
        query: The OSM query of the city/region to download

    """

    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    warnings.simplefilter('ignore', InsecureRequestWarning)
    
    auth = Authentication(verify=False)

    # The URL to the WCS service
    base_url = 'http://tinitaly.pi.ingv.it/TINItaly_1_1/wcs'

    # # Get the GeoDataFrame for the specified place
    gdf = ox.geocode_to_gdf(query)

    # # Extract the bounding box
    bbox = gdf['geometry'].bounds.iloc[0]
    print("Bounding Box:", bbox)

    # # Transform bounding box from EPSG:4326 (WGS 84) to EPSG:32632 (UTM zone 32N)
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:32632")
    minx, miny = transformer.transform(bbox.miny, bbox.minx)
    maxx, maxy = transformer.transform(bbox.maxy, bbox.maxx)

    # # Format the transformed bounding box values into a string
    bbox_str = f"{float(round(minx))},{float(round(miny))},{float(round(maxx))},{float(round(maxy))}"
    print(bbox_str)
    # Define the parameters for the GetCoverage request
    params = {
        'service': 'WCS',
        'version': '1.0.0',
        'request': 'GetCoverage',
        'coverage': 'TINItaly_1_1:tinitaly_slope',
        'bbox': bbox_str, 
        'crs': 'EPSG:32632',
        'format': 'image/tiff',
        'width': '5010',
        'height': '5010'
    }

    # Make the GetCoverage request
    response = requests.get(base_url, params=params, verify=False)

    # If the content type indicates it's a TIFF, save the response to a file
    if response.headers.get('Content-Type') == 'image/tiff':
        dem_path = base_folder + '/dem.tif'
        with open(dem_path, 'wb') as out_file:
            out_file.write(response.content)
        
        dem_content = open(dem_path, 'rb').read()
        context.log_artifact("dem_model", body=response.content, format="tif", db_key="dem_model")
        # return response.content
    else:
        print("The response is not a TIFF file.")
        raise Exception("The response is not a TIFF file.")

def fetch_building_data(query):
    """
    Fetches the OSM  buildings data for the city/region involved.
    args:
        query: The OSM query of the city/region to download
    """

    print(f"Fetching building data for {query}...")
    # Use features_from_place instead of geometries_from_place
    buildings = ox.features_from_place(query, tags={'building': True})
    buildings.drop(columns=["nodes"], inplace=True)
    print(f"Number of buildings fetched: {len(buildings)}")
    return buildings            

@mlrun.handler()
def download_osm(context, query: str):    
    """
    Downloads data from OpenStreetMap
    args:
        query: The OSM query of the city/region to download
    """
    base_folder = './data'
        
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    # Download the OSM data for the city/region involved.
    G = ox.graph_from_place(query, network_type="all")

    # Convert the graph into GeoDataFrames
    gdf_nodes, gdf_edges = ox.graph_to_gdfs(G)

    gdf_edges['osmid'] = gdf_edges['osmid'].apply(lambda x: x[0] if isinstance(x, list) else x)
    gdf_edges['lanes'] = gdf_edges['lanes'].astype(str)
    gdf_edges['ref'] = gdf_edges['ref'].astype(str)
    gdf_edges['name'] = gdf_edges['name'].astype(str)
    gdf_edges['highway'] = gdf_edges['highway'].astype(str)
    gdf_edges['maxspeed'] = gdf_edges['maxspeed'].astype(str)
    gdf_edges['reversed'] = gdf_edges['reversed'].apply(lambda x: True if x else False)
    gdf_edges['bridge'] = gdf_edges['bridge'].astype(str)
    gdf_edges['access'] = gdf_edges['access'].astype(str)
    gdf_edges['service'] = gdf_edges['service'].astype(str)
    gdf_edges['tunnel'] = gdf_edges['tunnel'].astype(str)
    gdf_edges['width'] = gdf_edges['width'].astype(str)

    buildings = fetch_building_data(query)
    
    gdf_nodes.to_parquet('./data/nodes.parquet')
    with open('./data/nodes.parquet', 'rb') as in_file:
        content = in_file.read()
        context.log_artifact("osm_nodes", body=content, format="parquet", db_key="osm_nodes")

    gdf_edges.to_parquet('./data/edges.parquet')
    with open('./data/edges.parquet', 'rb') as in_file:
        content = in_file.read()
        context.log_artifact("osm_edges", body=content, format="parquet", db_key="osm_edges")

    buildings.to_parquet('./data/buildings.parquet')
    with open('./data/buildings.parquet', 'rb') as in_file:
        content = in_file.read()
        context.log_artifact("osm_buildings", body=content, format="parquet", db_key="osm_buildings")



def calculate_building_distances(gdf_buildings):
    """
    Calculates the distance between each building and the nearest road
    args:
        gdf_buildings: The GeoDataFrame of the buildings
    """

    print(f"Calculating distances for {len(gdf_buildings)} buildings...")
    # Ensure correct CRS for distance calculations
    gdf_projected = gdf_buildings.to_crs(epsg=32632)
    building_coords = np.array(list(gdf_projected.geometry.centroid.apply(lambda x: (x.x, x.y))))
    
    nbrs = NearestNeighbors(n_neighbors=2, algorithm='ball_tree').fit(building_coords)
    distances, _ = nbrs.kneighbors(building_coords)
    print(f"Distances calculated: {distances[:, 1]}")
    return distances[:, 1]


def divide_into_quintiles(distances):
    """
    Divides the distances into quintiles
    """
    quintiles = np.percentile(distances, [20, 40, 60, 80, 100])
    return quintiles

def classify_edges_by_quintiles(gdf_edges, gdf_buildings, quintiles):
    """
    Classifies edges by their distance to the nearest road
    args:
        gdf_edges: The GeoDataFrame of the edges
        gdf_buildings: The GeoDataFrame of the buildings
        quintiles: The quintiles of the distances
    """
    urban_threshold = quintiles[2]  # Third quintile
    print(f"Urban threshold set at {urban_threshold} units")
    gdf_edges['context'] = 'countryside'  # Default to countryside

    gdf_buildings = gdf_buildings.to_crs(gdf_edges.crs)

    for index, edge in gdf_edges.iterrows():
        edge_centroid = edge.geometry.centroid
        buffer = edge_centroid.buffer(urban_threshold)
        buffer_gdf = gpd.GeoDataFrame(geometry=[buffer], crs=gdf_edges.crs)
        possible_matches = gpd.sjoin(gdf_buildings, buffer_gdf, how='inner', predicate='intersects')
        
        if not possible_matches.empty:
            gdf_edges.at[index, 'context'] = 'urban'
            print(f"Edge {index} classified as urban")

def sample_slope_at_points(geometry, dtm):
    """
    Samples the slope at the points of the geometry
    args:
        geometry: The geometry of the line
        dtm: The Digital Terrain Model
    """

    # Campionamento del valore di pendenza nel punto medio della linea
    midpoint = geometry.interpolate(0.5, normalized=True)
    # Assumendo che i valori di slope siano in percentuale, dividi per 100 per ottenere valori frazionari
    slope_value = dtm.sel(x=midpoint.x, y=midpoint.y, method="nearest").item() / 100
    return slope_value

def slope_class_label(slope):
    """
    Returns the class label of the slope (from flat to impossible)
    args:
        slope: The slope value
    """
    if slope < 0.03:
        return 'flat'
    elif slope < 0.05:
        return 'mild'
    elif slope < 0.08:
        return 'medium'
    elif slope < 0.1:
        return 'hard'
    elif slope < 0.2:
        return 'extreme'
    else:
        return 'impossible'

def slope_class(slope):
    """
    Returns the class of the slope (from 0 to 5)
    args:
        slope: The slope value
    """

    if slope < 0.03:
        return 0
    elif slope < 0.05:
        return 1
    elif slope < 0.08:
        return 2
    elif slope < 0.1:
        return 3
    elif slope < 0.2:
        return 4
    else:
        return 5

def calc_slope(gdf_edges, dem_path):
    """
    Calculates the slope of each edge
    args:
        gdf_edges: The GeoDataFrame of the edges    
        dem_path: The path to the Digital Elevation Model   
    """

    with rasterio.open(dem_path, 'r') as ds:
        dtm_data = ds.read(1)  # read all raster values
        # Calcola le coordinate
        # np.arange genera un array di valori che iniziano da 0 fino alla dimensione meno uno,
        # che poi viene trasformato dalle coordinate dell'angolo superiore sinistro e dalla dimensione del pixel
        x = np.arange(ds.width) * ds.transform[0] + ds.transform[2]
        y = np.arange(ds.height) * ds.transform[4] + ds.transform[5]
        
        # Crea un xarray DataArray
        dtm_xr = xr.DataArray(
            data=dtm_data,
            dims=["y", "x"],
            coords={
                "x": ("x", x),
                "y": ("y", y)
            }
        )
        dtm_xr.rio.write_crs(ds.crs.to_string(), inplace=True)
        dtm_xr.rio.write_transform(ds.transform, inplace=True) 

    # Calcola la pendenza per ciascuna strada utilizzando il punto medio
    gdf_edges['slope'] = gdf_edges['geometry'].apply(lambda geom: sample_slope_at_points(geom, dtm_xr))
    gdf_edges[gdf_edges['slope'] == gdf_edges['slope'].min()][['osmid','name','slope']]
    gdf_edges['slope_class'] = gdf_edges['slope'].apply(lambda slope: slope_class(slope))
    gdf_edges['slope_class_label'] = gdf_edges['slope'].apply(lambda slope: slope_class_label(slope))

@mlrun.handler()
def merge_osm_dem(context, nodes: mlrun.DataItem, edges: mlrun.DataItem, buildings: mlrun.DataItem, dem: mlrun.DataItem):    
    """
    Merge OSM graph with elevation data
    args:
        nodes: The OSM nodes GeoDataFrame of the city/region to download
        edges: The OSM edges GeoDataFrame of the city/region to download
        dem: Elevation data
    """
    base_folder = './data'
        
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    # Fetch OSM data
    nodes.download(base_folder + "/osm_nodes.parquet")
    gdf_nodes = gpd.read_parquet(base_folder + "/osm_nodes.parquet") # pickle.load(open(base_folder + "/osm_nodes.json", "rb"))
    edges.download(base_folder + "/osm_edges.parquet")
    gdf_edges = gpd.read_parquet(base_folder + "/osm_edges.parquet") # pickle.load(open(base_folder + "/osm_edges.json", "rb"))
    buildings.download(base_folder + "/osm_buildings.parquet")
    gdf_buildings = gpd.read_parquet(base_folder + "/osm_buildings.parquet") # pickle.load(open(base_folder + "/osm_buildings.jsob", "rb"))
    # Fetch elevation data
    dem.download(base_folder + "/dem.tif")
    tif_url = base_folder + "/dem.tif"

    bb = gdf_edges.total_bounds
    distances = calculate_building_distances(gdf_buildings)
    quintiles = divide_into_quintiles(distances)

    # Ensure gdf_edges is in the same projected CRS for accurate distance calculations
    gdf_edges_projected = gdf_edges.to_crs(epsg=32632)
    # gdf_edges_classified = classify_edges_by_quintiles(gdf_edges_projected, gdf_buildings, quintiles)
    # !WORKAROUND: gdf_edges_classified is not used in the pipeline
    gdf_edges_classified = gdf_edges_projected
    gdf_edges_classified['context'] = 'urban'

    # Convert back to original CRS if needed
    gdf_edges = gdf_edges_classified.to_crs(gdf_edges.crs)

    # Store the original MultiIndex
    original_index = gdf_edges.index
    
    # Transform the gdf_edges using the SlopeCalculator
    calc_slope(gdf_edges_projected, tif_url)
    
    # Reassign the original MultiIndex to gdf_edges2
    gdf_edges_projected.index = original_index
    
    # Reproject to WGS84
    gdf_edges = gdf_edges_projected.to_crs(epsg=4326)

    # Create a colormap for slope classes
    color_palette = ["#267300", "#70A800", "#FFAA00", "#E60000", "#A80000", "#730000"]
    slope_classes = ["flat", "mild", "medium", "hard", "extreme", "impossible"]
    colors = dict(zip(slope_classes, color_palette))

    # Calculate the mean of latitudes and longitudes
    mean_latitude = gdf_edges.geometry.apply(lambda geom: geom.centroid.y).mean()
    mean_longitude = gdf_edges.geometry.apply(lambda geom: geom.centroid.x).mean()

    # Create a folium map centered on the mean of latitudes and longitudes
    map_osm = folium.Map(location=[mean_latitude, mean_longitude], zoom_start=11)

    # Add slope information to the map
    for _, row in gdf_edges.iterrows():
        color = colors.get(str(row['slope_class_label']), "#000000")  # default color is black
        folium.GeoJson(
            row['geometry'], 
            style_function=lambda _, color=color: {'color': color}  # use default argument to capture color
        ).add_to(map_osm)

    # Create a custom legend HTML string
    legend_html = '''
    <div style="position: fixed; top: 10px; right: 10px; z-index: 1000; background-color: white; padding: 5px; border: 1px solid grey; font-size: 12px;">
    <p><b>Slope</b></p>
    '''
    for slope_class, color in colors.items():
        legend_html += f'<p><i class="fa fa-square" style="color:{color};"></i> {slope_class}</p>'
    legend_html += '</div>'

    # Add the legend HTML to the map
    map_osm.get_root().html.add_child(folium.Element(legend_html))
    file_path = os.path.join(base_folder, 'slope_map.html')
    map_osm.save(file_path)
    map_content = open(file_path, 'r').read()
    context.log_artifact('slope_map', body=map_content, format="html", db_key="slope_map")

    gdf_edges.to_parquet('./data/edges.parquet')
    with open('./data/edges.parquet', 'rb') as in_file:
        content = in_file.read()
        context.log_artifact("osm_edges_elevation", body=content, format="parquet", db_key="osm_edges_elevation")

    # return json.loads(gdf_edges.to_json()) 