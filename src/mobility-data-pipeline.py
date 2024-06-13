from kfp import dsl
import mlrun

@dsl.pipeline(name="Mobility data preparation pipeline")
def pipeline(query: str, name: str):
    # Get the current project
    project = mlrun.get_current_project()

    step1 = project.run_function("download-dem", params={"query": "Bologna"}, outputs=["dem_model"])
    
    step2 = project.run_function("download-osm", params={"query": "Bologna"}, outputs=["osm_nodes", "osm_edges", "osm_buildings"])

    step3 = project.run_function("merge-osm-dem", inputs={
        "nodes": step2.outputs["osm_nodes"], 
        "edges": step2.outputs["osm_edges"], 
        "buildings": step2.outputs["osm_buildings"], 
        "dem": step1.outputs["dem_model"]}, outputs=["slope_map", "osm_edges_elevation"])
    
    step4 = project.run_function("download-road-areas", outputs=["ctm_road_areas"])
    
    step5 = project.run_function("download-road-edges", outputs=["rifter_edges"])

    step6 = project.run_function("download-road-nodes", outputs=["rifter_nodes"])

    step7 = project.run_function("download-curves", outputs=["ctm_level_curves_10m"])

    step8 = project.run_function("download-sidewalks", outputs=["ctm_level_sidewalks"])

    step9 = project.run_function("download-city30", outputs=["city_30"])

    project.run_function("share", params={"bucket": "dataspace", "path":"mobility-data"})
            .after(step1, step2, step3, step4, step5, step6, step7, step8, step9)
    
    