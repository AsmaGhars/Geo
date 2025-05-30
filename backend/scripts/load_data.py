import geopandas as gpd
import asyncio

async def load_data(pb_path, zpbo_path, zsro_path, zpa_path, cb_di_path, pa_path, znro_path, adresse_path, cm_di_path, support_path, sro_path, nro_path, pep_di_path, creation_conduite_di_path):
    loop = asyncio.get_event_loop()
    pb_gdf = await loop.run_in_executor(None, gpd.read_file, pb_path)
    zpbo_gdf = await loop.run_in_executor(None, gpd.read_file, zpbo_path)
    zsro_gdf = await loop.run_in_executor(None, gpd.read_file, zsro_path)
    zpa_gdf = await loop.run_in_executor(None, gpd.read_file, zpa_path)
    cb_di_gdf = await loop.run_in_executor(None, gpd.read_file, cb_di_path)
    pa_gdf = await loop.run_in_executor(None, gpd.read_file, pa_path)
    znro_gdf = await loop.run_in_executor(None, gpd.read_file, znro_path)
    adresse_gdf = await loop.run_in_executor(None, gpd.read_file, adresse_path)
    cm_di_gdf = await loop.run_in_executor(None, gpd.read_file, cm_di_path)
    support_gdf = await loop.run_in_executor(None, gpd.read_file, support_path)
    sro_gdf = await loop.run_in_executor(None, gpd.read_file, sro_path)
    nro_gdf = await loop.run_in_executor(None, gpd.read_file, nro_path)
    pep_di_gdf = await loop.run_in_executor(None, gpd.read_file, pep_di_path)
    creation_conduite_di_gdf = await loop.run_in_executor(None, gpd.read_file, creation_conduite_di_path)
    
    return pb_gdf, zpbo_gdf, zsro_gdf, zpa_gdf, cb_di_gdf, pa_gdf, znro_gdf, adresse_gdf, cm_di_gdf, support_gdf, sro_gdf, nro_gdf, pep_di_gdf, creation_conduite_di_gdf