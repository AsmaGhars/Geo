from shapely import wkt
from shapely.geometry import Point, LineString, MultiPoint, MultiPolygon, GeometryCollection, Polygon
from shapely.ops import unary_union
import pandas as pd
import re, os
import geopandas as gpd
from datetime import datetime
from ..metrics import *

def convert_geometries_to_wkt(gdf):
    gdf['wkt'] = gdf['geometry'].apply(lambda geom: wkt.dumps(geom))
    return gdf

async def verify_geometries_in_zones(gdf, zgdf, zone_type):
    if gdf.crs != zgdf.crs:
        gdf = gdf.to_crs(zgdf.crs)

    gdf = convert_geometries_to_wkt(gdf)
    zgdf = convert_geometries_to_wkt(zgdf)

    if zone_type in ['PA', 'PB']:
        z_dict = {row['pcn_code']: wkt.loads(row['wkt']) for _, row in zgdf.iterrows()}
    elif zone_type == 'SRO':
        z_dict = {row['zs_code']: (wkt.loads(row['wkt']), row['zs_nd_code']) for _, row in zgdf.iterrows()}
    elif zone_type == 'NRO':
        z_dict = {row['zn_code']: (wkt.loads(row['wkt']), row['zn_nd_code']) for _, row in zgdf.iterrows()}
    else:
        raise ValueError("Le paramètre 'zone_type' doit être 'PA', 'PB', 'SRO' ou 'NRO'.")

    not_in_zones = []
    nd_code_mismatch = []
    rows_outside_zone = []
    rows_nd_code_mismatch = []

    for _, row in gdf.iterrows():
        geom = wkt.loads(row['wkt'])
        if zone_type in ['PA', 'PB']:
            z_geom = z_dict.get(row['pcn_code'])
            if not z_geom or not z_geom.contains(geom):
                not_in_zones.append(row['pcn_code'])
                rows_outside_zone.append(row)
        else:
            nd_code = row['nd_code']
            z_data = next(((zg, zc) for zc, (zg, znd) in z_dict.items() if znd == nd_code), None)

            if not z_data:
                not_in_zones.append(nd_code)
                rows_outside_zone.append(row)
                continue

            z_geom, z_code = z_data

            if not z_geom.contains(geom):
                not_in_zones.append(nd_code)
                rows_outside_zone.append(row)
            elif nd_code != z_dict[z_code][1]:
                nd_code_mismatch.append(nd_code)
                rows_nd_code_mismatch.append(row)

    if rows_outside_zone:
        export_outside_gdf = gpd.GeoDataFrame(rows_outside_zone, crs=gdf.crs)
        export_outside_gdf["geometry"] = export_outside_gdf["wkt"].apply(wkt.loads)

        downloads = os.path.join(os.path.expanduser("~"), "Downloads")
        os.makedirs(downloads, exist_ok=True)

        shp_out_path = os.path.join(downloads, f"{zone_type.lower()}_outside_zone.shp")
        gpkg_out_path = os.path.join(downloads, f"{zone_type.lower()}_outside_zone.gpkg")

        export_outside_gdf.to_file(shp_out_path, driver='ESRI Shapefile', encoding='UTF-8')
        export_outside_gdf.to_file(gpkg_out_path, layer=f"{zone_type.lower()}_outside_zone", driver='GPKG')
        print(f"Entités hors zones exportées :\n- SHP: {shp_out_path}\n- GPKG: {gpkg_out_path}")

    if rows_nd_code_mismatch:
        export_mismatch_gdf = gpd.GeoDataFrame(rows_nd_code_mismatch, crs=gdf.crs)
        export_mismatch_gdf["geometry"] = export_mismatch_gdf["wkt"].apply(wkt.loads)

        shp_mis_path = os.path.join(downloads, f"{zone_type.lower()}_nd_code_mismatch.shp")
        gpkg_mis_path = os.path.join(downloads, f"{zone_type.lower()}_nd_code_mismatch.gpkg")

        export_mismatch_gdf.to_file(shp_mis_path, driver='ESRI Shapefile', encoding='UTF-8')
        export_mismatch_gdf.to_file(gpkg_mis_path, layer=f"{zone_type.lower()}_nd_code_mismatch", driver='GPKG')
        print(f"Entités avec nd_code incorrect exportées :\n- SHP: {shp_mis_path}\n- GPKG: {gpkg_mis_path}")

    if not_in_zones:
        ANOMALY_COUNT.inc(len(not_in_zones))
        for code in not_in_zones:
            NOT_IN_ZONE.labels(zone_type=zone_type, code=code).set(1)

    if nd_code_mismatch:
        print(f"Le(s) {zone_type}(s) suivants ont un nd_code différent de z_nd_code :", nd_code_mismatch)
        ANOMALY_COUNT.inc(len(nd_code_mismatch))

    return not_in_zones, nd_code_mismatch

async def check_zp_intersections(zp_gdf, x):

    if zp_gdf.crs is None:
        raise ValueError(f"Le GeoDataFrame des Z{x} doit avoir un CRS défini.")
    crs = zp_gdf.crs

    records = []
    for i, row1 in zp_gdf.iterrows():
        for j, row2 in zp_gdf.iloc[i+1:].iterrows():
            raw = row1.geometry.intersection(row2.geometry)
            if raw.is_empty:
                continue
            geoms = []
            if isinstance(raw, (Polygon, MultiPolygon)):
                geoms = [raw] if isinstance(raw, Polygon) else list(raw.geoms)
            elif isinstance(raw, GeometryCollection):
                for geom in raw.geoms:
                    if isinstance(geom, (Polygon, MultiPolygon)):
                        geoms.extend([geom] if isinstance(geom, Polygon) else list(geom.geoms))
            if not geoms:
                continue
            inter_geom = geoms[0] if len(geoms) == 1 else MultiPolygon(geoms)

            code1 = row1['zs_code'] if x == 'SRO' else row1['pcn_code']
            code2 = row2['zs_code'] if x == 'SRO' else row2['pcn_code']
            records.append({
                'code1': code1,
                'code2': code2,
                'wkt': inter_geom.wkt
            })

    if not records:
        print(f"Aucune zone d'intersection détectée pour Z{x}.")
        return []

    inter_gdf = gpd.GeoDataFrame([
        {'code1': r['code1'], 'code2': r['code2'], 'geometry': wkt.loads(r['wkt'])} for r in records
    ], crs=crs)

    downloads = os.path.join(os.path.expanduser("~"), "Downloads")
    os.makedirs(downloads, exist_ok=True)
    shp_path = os.path.join(downloads, f"zp_{x.lower()}_intersect.shp")
    gpkg_path = os.path.join(downloads, f"zp_{x.lower()}_intersect.gpkg")

    inter_gdf.to_file(shp_path, driver='ESRI Shapefile', encoding='UTF-8', engine='fiona')
    inter_gdf.to_file(gpkg_path, layer=f"zp_{x.lower()}_intersect", driver='GPKG')

    print(f"Couches d'intersection Z{x} exportées :\n- SHP: {shp_path}\n- GPKG: {gpkg_path}")
    ANOMALY_COUNT.inc(len(records))

    return records

async def verify_zsro_in_zonenro(zsro_gdf, znro_gdf):
   
    if zsro_gdf.crs != znro_gdf.crs:
        zsro_gdf = zsro_gdf.to_crs(znro_gdf.crs)

    znro_geom = znro_gdf.unary_union
    records = []
    for idx, row in zsro_gdf.iterrows():
        diff = row.geometry.difference(znro_geom)
        if diff.is_empty:
            continue
        parts = []
        if isinstance(diff, Polygon):
            parts = [diff]
        elif isinstance(diff, MultiPolygon):
            parts = list(diff.geoms)
        else:
            continue
        for part in parts:
            records.append({'zs_code': row['zs_code'], 'wkt': part.wkt})
            ZSRO_NOT_IN_ZNRO.labels(zone_type='ZSRO', code=row['zs_code']).set(1)

    if not records:
        print("Aucune partie de ZSRO n’est hors de la ZNRO.")
        return []

    export_gdf = gpd.GeoDataFrame([
        {'zs_code': r['zs_code'], 'geometry': wkt.loads(r['wkt'])} for r in records
    ], crs=zsro_gdf.crs)
    downloads = os.path.join(os.path.expanduser("~"), "Downloads")
    os.makedirs(downloads, exist_ok=True)
    export_gdf.to_file(os.path.join(downloads, "zsro_not_in_znro.shp"), driver='ESRI Shapefile', encoding='UTF-8', engine='fiona')
    export_gdf.to_file(os.path.join(downloads, "zsro_not_in_znro.gpkg"), layer="zsro_not_in_znro", driver='GPKG')
    ANOMALY_COUNT.inc(len(records))
    return records

async def detect_self_intersections_c(c_gdf, type):
    if c_gdf.crs is None:
        raise ValueError(f"Le GeoDataFrame des {type} doit avoir un système de coordonnées (CRS) défini.")

    code_attr = 'cm_codeext' if type == 'CM' else 'cl_codeext'

    if code_attr in c_gdf.columns:
        c_gdf[code_attr] = c_gdf[code_attr].astype(str)

    self_int_codes = []
    rows_to_export = []

    for idx, row in c_gdf.iterrows():
        geom = row.geometry
        code = row[code_attr]
        if isinstance(geom, LineString):
            if not geom.is_valid or not geom.is_simple:
                self_int_codes.append(code)
                rows_to_export.append(row)
                status = "invalide" if not geom.is_valid else "auto-intersectante"
                print(f"{type} {code} a une géométrie {status}.")
        else:
            print(f"{type} {code} n'est pas une LineString.")

    if self_int_codes:
        ANOMALY_COUNT.inc(len(self_int_codes))
        print(f"Les {type} suivants ont des auto-intersections ou sont invalides :")
        for code in self_int_codes:
            print(f"- {code}")

        
        export_gdf = gpd.GeoDataFrame(rows_to_export, crs=c_gdf.crs)

        downloads = os.path.join(os.path.expanduser("~"), "Downloads")
        os.makedirs(downloads, exist_ok=True)

        shp_path = os.path.join(downloads, f"{type.lower()}_self_intersections.shp")
        gpkg_path = os.path.join(downloads, f"{type.lower()}_self_intersections.gpkg")

        export_gdf.to_file(shp_path, driver='ESRI Shapefile', encoding='UTF-8')
        print(f"Shapefile exporté dans : {shp_path}")

        export_gdf.to_file(gpkg_path, layer=f"{type.lower()}_self_intersections", driver='GPKG')
        print(f"GeoPackage exporté dans : {gpkg_path}")

    return self_int_codes

async def verify_c_intersections(c_di_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf, adresse_gdf, type_):
    if c_di_gdf.crs is None:
        raise ValueError("Le GeoDataFrame des CB doit avoir un système de coordonnées (CRS) défini.")

    for gdf in [support_gdf, pb_gdf, pa_gdf, sro_gdf, adresse_gdf]:
        if gdf.crs != c_di_gdf.crs:
            gdf.to_crs(c_di_gdf.crs, inplace=True)

    if type_ == 'CB':
        code_attr = 'cl_codeext'
    elif type_ == 'CM':
        code_attr = 'cm_codeext'
    else:
        raise ValueError(f"Type inconnu: {type_}. Les types valides sont 'CB' et 'CM'.")

    export_records = []
    code_pairs = []

    for i, row1 in c_di_gdf.iterrows():
        for j, row2 in c_di_gdf.iloc[i+1:].iterrows():
            if row1.geometry.intersects(row2.geometry) and not row1.geometry.touches(row2.geometry):
                raw = row1.geometry.intersection(row2.geometry)
                pts = []
                if isinstance(raw, MultiPoint):
                    pts = list(raw.geoms)
                elif isinstance(raw, Point):
                    pts = [raw]
                else:
                    continue

                for pt in pts:
                    if not (
                        pb_gdf.geometry.touches(pt).any() or
                        pa_gdf.geometry.touches(pt).any() or
                        sro_gdf.geometry.touches(pt).any() or
                        adresse_gdf.geometry.touches(pt).any() or
                        support_gdf.geometry.contains(pt).any() or support_gdf.geometry.touches(pt).any()
                    ):
                        val1 = row1[code_attr]
                        val2 = row2[code_attr]
                        export_records.append({
                            'code1': val1,
                            'code2': val2,
                            'geometry': pt
                        })
                        code_pairs.append((val1, val2))

    if export_records:
        inter_gdf = gpd.GeoDataFrame(export_records, crs=c_di_gdf.crs)

        downloads = os.path.join(os.path.expanduser("~"), "Downloads")
        os.makedirs(downloads, exist_ok=True)

        shp_path = os.path.join(downloads, f"{type_.lower()}_intersections.shp")
        gpkg_path = os.path.join(downloads, f"{type_.lower()}_intersections.gpkg")

        inter_gdf[['code1', 'code2', 'geometry']].to_file(
            shp_path,
            driver='ESRI Shapefile',
            encoding='UTF-8',
            engine='fiona'
        )
        inter_gdf.to_file(
            gpkg_path,
            layer=f"{type_.lower()}_intersections",
            driver='GPKG'
        )

        print(f"Couche d'intersections exportée :\n- SHP: {shp_path}\n- GPKG: {gpkg_path}")
    else:
        print(f"Aucune intersection isolée détectée pour le type {type_}.")

    return code_pairs

async def verify_mic_pm(zsro_gdf):

    invalid_pms = []
    pm_invalid = zsro_gdf[zsro_gdf['pcn_umtot'] > 90]
    
    for _, row in pm_invalid.iterrows():
        code = row['zs_code']
        um_value = row['pcn_umtot']
        SRO_UMTOT_EXCESS.labels(zs_code=code).set(um_value)
        invalid_pms.append((code, um_value))
    if invalid_pms:
        ANOMALY_COUNT.inc(len(invalid_pms))
        print("PMs dépassant 90 µm FTTH par PM :")
        for code, um_value in invalid_pms:
            print(f"- {code} : {um_value} µm")
    
    return invalid_pms

async def detect_cb_without_cm(cb_di_gdf, cm_di_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf):
    gdfs = [cm_di_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf]
    for gdf in gdfs:
        if cb_di_gdf.crs != gdf.crs:
            gdf = gdf.to_crs(cb_di_gdf.crs)

    cb_sans_cm = []

    for idx_cb, cb_row in cb_di_gdf.iterrows():
        cb_geom = cb_row.geometry

        match_found = any(cb_geom.equals(cm_geom) for cm_geom in cm_di_gdf.geometry)

        if not match_found:
            cm_union = unary_union(cm_di_gdf.geometry)
            cb_diff_cm = cb_geom.difference(cm_union)

            supports_in_cb = support_gdf[support_gdf.geometry.within(cb_geom)]
            pbs_in_cb = pb_gdf[pb_gdf.geometry.within(cb_geom)]
            pas_in_cb = pa_gdf[pa_gdf.geometry.within(cb_geom)]
            sros_in_cb = sro_gdf[sro_gdf.geometry.within(cb_geom)]

            all_elements_geom = unary_union(
                list(supports_in_cb.geometry) + 
                list(pbs_in_cb.geometry) + 
                list(pas_in_cb.geometry) + 
                list(sros_in_cb.geometry)
            )

            uncovered_area = cb_diff_cm.difference(all_elements_geom)

            if not uncovered_area.is_empty:
                cb_sans_cm.append(cb_row['cl_codeext'])

    if cb_sans_cm:
        cb_sans_cm_gdf = cb_di_gdf[cb_di_gdf['cl_codeext'].isin(cb_sans_cm)].copy()
        print("Les CB suivants ne disposent d’aucun CM :")
        ANOMALY_COUNT.inc(len(cb_sans_cm))
        for cb in cb_sans_cm:
            print(f"- {cb}")
        for _, row in cb_sans_cm_gdf[['cl_codeext','nd_r4_code']].iterrows():
            print(f"Exporter : cl_codeext={row['cl_codeext']} — nd_r4_code={row['nd_r4_code']}")
        
        downloads = os.path.join(os.path.expanduser("~"), "Downloads")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        shp = os.path.join(downloads, f"cb_sans_cm_{timestamp}.shp")
        
        try:
            cb_sans_cm_gdf.to_file(shp, driver='ESRI Shapefile', encoding='UTF-8')
            print(f"Shapefile exporté dans {shp}")
        except PermissionError as e:
            print(f"Erreur de permission lors de l'écriture du fichier : {e}")
        
    return cb_sans_cm

async def check_column_duplicates(data, column_name, file_key):
    duplicates = data[data.duplicated(subset=[column_name], keep=False)]
    if not duplicates.empty:
        ANOMALY_COUNT.inc(len(duplicates))
        unique_duplicates = set(duplicates[column_name].tolist())
        for value in unique_duplicates:
            DUPLICATE_COUNT.labels(file_key=file_key, column_name=column_name, duplicate_value=value).set(len(duplicates[duplicates[column_name] == value]))
        print(f"Doublons trouvés dans le fichier {file_key} pour la colonne {column_name}:")
        print(list(unique_duplicates))

async def check_duplicates(dataframes):
    try:
        for file_key, data in dataframes:
            if file_key == "CB_DI" and 'cl_codeext' in data.columns:
                await check_column_duplicates(data, 'cl_codeext', file_key)
            if file_key == "CM_DI" and 'cm_codeext' in data.columns:
                await check_column_duplicates(data, 'cm_codeext', file_key)
            if file_key == "PB" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "ADRESSE" and 'ad_code' in data.columns:
                await check_column_duplicates(data, 'ad_code', file_key)
            if file_key == "NRO" and 'nd_code' in data.columns:
                await check_column_duplicates(data, 'nd_code', file_key)
            if file_key == "PA" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "PEP" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "SRO" and 'nd_code' in data.columns:
                await check_column_duplicates(data, 'nd_code', file_key)
            if file_key == "SUPPORT" and 'pt_codeext' in data.columns:
                await check_column_duplicates(data, 'pt_codeext', file_key)
            if file_key == "SUPPORT" and 'pcn_id' in data.columns:
                await check_column_duplicates(data, 'pcn_id', file_key)
            if file_key == "ZNRO" and 'zn_code' in data.columns:
                await check_column_duplicates(data, 'zn_code', file_key)
            if file_key == "ZPA" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "ZPBO" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "ZSRO" and 'zs_code' in data.columns:
                await check_column_duplicates(data, 'zs_code', file_key)
    except Exception as e:
        print(f"Erreur lors de la vérification des doublons : {e}")

async def verify_cable_direction(cb_di_gdf, nro_gdf, sro_gdf, pa_gdf, pb_gdf, adresse_gdf):
    def get_zone_type(geom, zone_gdfs):
        for zone_type, zone_gdf in zone_gdfs.items():
            if zone_gdf.geometry.contains(geom).any():
                return zone_type
        return None

    zone_gdfs = {
        'NRO': nro_gdf,
        'SRO': sro_gdf,
        'PA': pa_gdf,
        'PB': pb_gdf,
        'ADRESSE': adresse_gdf
    }

    incorrect_cables = []

    for _, row in cb_di_gdf.iterrows():
        geom = row['geometry']
        if not isinstance(geom, LineString):
            continue

        source = Point(geom.coords[0])
        destination = Point(geom.coords[-1])

        source_zone_type = get_zone_type(source, zone_gdfs)
        destination_zone_type = get_zone_type(destination, zone_gdfs)

        if source_zone_type and destination_zone_type:
            if source_zone_type == destination_zone_type:
                continue
            elif source_zone_type == 'NRO' and destination_zone_type == 'SRO':
                continue
            elif source_zone_type == 'SRO' and destination_zone_type == 'PA':
                continue
            elif source_zone_type == 'PA' and destination_zone_type == 'PB':
                continue
            elif source_zone_type == 'PB' and destination_zone_type == 'ADRESSE':
                continue

        incorrect_cables.append({
            'cl_codeext': row['cl_codeext'],
            'geometry': geom 
        })
        ZPA_NOT_IN_ZSRO.labels(zone_type='CABLE', code=row['cl_codeext']).set(1)

    if not incorrect_cables:
        print("Tous les câbles ont un sens correct.")
        return []

    incorrect_cables_json = [
        {
            'cl_codeext': cable['cl_codeext'],
            'geometry': cable['geometry'].__geo_interface__  
        }
        for cable in incorrect_cables
    ]

    incorrect_gdf = gpd.GeoDataFrame(incorrect_cables, crs=cb_di_gdf.crs)

    downloads = os.path.join(os.path.expanduser("~"), "Downloads")
    os.makedirs(downloads, exist_ok=True)

    incorrect_gdf.to_file(os.path.join(downloads, "cables_incorrect_direction.shp"), driver='ESRI Shapefile', encoding='UTF-8', engine='fiona')
    incorrect_gdf.to_file(os.path.join(downloads, "cables_incorrect_direction.gpkg"), layer="cables_incorrect_direction", driver='GPKG')

    ANOMALY_COUNT.inc(len(incorrect_cables))
    print("Les câbles suivants ont un sens incorrect :")
    for cable in incorrect_cables:
        print(f"- {cable['cl_codeext']}")

    return incorrect_cables_json

#Table attributaire du NRO

async def verify_nd_code(gdf, table_type):
    missing_nd_code = gdf['nd_code'].isna() | (gdf['nd_code'] == '')
    
    if missing_nd_code.any():
        print(f"La colonne nd_code n'est pas remplie dans la table attributaire de {table_type}")
        return True
    else:
        return False

async def verify_nd_r3_code(nro_gdf):
    missing_nd_r3_code = nro_gdf['nd_r3_code'].isna() | (nro_gdf['nd_r3_code'] == '')
    
    if missing_nd_r3_code.any():
        print("La colonne nd_r3_code n'est pas remplie dans la table attributaire de NRO")
        return True
    else:
        return False

#Table attributaire deu ZNRO

async def verify_zn_code(znro_gdf):
    missing_nd_code = znro_gdf['zn_code'].isna() | (znro_gdf['zn_code'] == '')
    
    if missing_nd_code.any():
        print("La colonne zn_code n'est pas remplie dans la table attributaire de ZNRO")
        return True
    else:
        return False

async def verify_zn_nd_code(znro_gdf, nro_gdf):
    missing_mask = znro_gdf['zn_nd_code'].isna() | (znro_gdf['zn_nd_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zn_nd_code' contient des valeurs manquantes dans la table ZNRO")

    valid_codes = set(nro_gdf['nd_code'].dropna().unique())

    mismatch_mask = (~znro_gdf['zn_nd_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = znro_gdf.loc[mismatch_mask, 'zn_nd_code'].tolist()

    if invalid_codes:
        print("Les ZNRO suivants ont un 'zn_nd_code' non trouvé dans la table NRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zn_r1_code(znro_gdf):
    missing_zn_r1_code = znro_gdf['zn_r1_code'].isna() | (znro_gdf['zn_r1_code'] == '')
    
    if missing_zn_r1_code.any():
        print("La colonne zn_r1_code n'est pas remplie dans la table attributaire de ZNRO")
        return True
    else:
        return False

async def verify_zn_r2_code(znro_gdf):
    missing_zn_r2_code = znro_gdf['zn_r2_code'].isna() | (znro_gdf['zn_r2_code'] == '')
    
    if missing_zn_r2_code.any():
        print("La colonne zn_r2_code n'est pas remplie dans la table attributaire de ZNRO")
        return True
    else:
        return False

async def verify_zn_r3_code(znro_gdf, nro_gdf):
    missing_mask = znro_gdf['zn_r3_code'].isna() | (znro_gdf['zn_r3_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zn_r3_code' contient des valeurs manquantes dans la table ZNRO")

    valid_codes = set(nro_gdf['nd_r3_code'].dropna().unique())

    mismatch_mask = (~znro_gdf['zn_r3_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = znro_gdf.loc[mismatch_mask, 'zn_r3_code'].tolist()

    if invalid_codes:
        print("Les ZNRO suivants ont un 'zn_r3_code' non trouvé dans la table NRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zn_nroref(znro_gdf):
    if 'zn_nroref' not in znro_gdf.columns:
        raise KeyError("Colonne 'zn_nroref' absente de la table ZNRO")

    missing_mask = znro_gdf['zn_nroref'].isna() | (znro_gdf['zn_nroref'] == '')
    if missing_mask.any():
        print("La colonne 'zn_nroref' contient des valeurs manquantes ou vides dans ZNRO")

    pattern = re.compile(r'^\d{5}/NRO/[A-Z]{3}$')
    bad_format_mask = (~znro_gdf['zn_nroref'].str.match(pattern, na=False)) & (~missing_mask)  
    invalid = [(idx, val) 
               for idx, val in zip(znro_gdf.index[bad_format_mask], znro_gdf.loc[bad_format_mask, 'zn_nroref'])]
    if invalid:
        print("La ZNRO a un 'zn_nroref' mal formé (attendu '12345/NRO/ABC') :")
    return invalid

#Table attributaire du SRO
    
async def verify_nd_r4_code(sro_gdf):
    missing_nd_r4_code = sro_gdf['nd_r4_code'].isna() | (sro_gdf['nd_r4_code'] == '')
    
    if missing_nd_r4_code.any():
        print("La colonne nd_r4_code n'est pas remplie dans la table attributaire de SRO")
        return True
    else:
        return False

async def verify_pcn_cb_ent_sro(sro_gdf, adresse_gdf):
    invalid_pcn_cb_ent = []
    
    total_pcn_ftth = adresse_gdf['pcn_ftth'].sum()
    calculated_value = total_pcn_ftth / 6
    
    possible_values = [36, 72, 144]
    expected_pcn_cb_ent = min([val for val in possible_values if val >= calculated_value], default=None)
    
    if expected_pcn_cb_ent is None:
        print("Aucune valeur valide trouvée pour pcn_cb_ent.")
        return True
    
    for i, row in sro_gdf.iterrows():
        pcn_cb_ent = row['pcn_cb_ent']
        nd_code = row['nd_code']
        
        if pd.isna(pcn_cb_ent) or pcn_cb_ent == 0:
            print(f"La colonne pcn_cb_ent n'est pas remplie pour le SRO avec nd_code {nd_code}.")
            invalid_pcn_cb_ent.append(nd_code)
        elif pcn_cb_ent != expected_pcn_cb_ent:
            invalid_pcn_cb_ent.append(nd_code)
            print(f"SRO {nd_code} has pcn_cb_ent {pcn_cb_ent} which does not match the expected value {expected_pcn_cb_ent}.")
    
    return invalid_pcn_cb_ent

#Table attributaire du ZSRO

async def verify_zs_code(zsro_gdf):
    missing_zs_code = zsro_gdf['zs_code'].isna() | (zsro_gdf['zs_code'] == '')
    
    if missing_zs_code.any():
        print("La colonne zs_code n'est pas remplie dans la table attributaire de ZSRO")
        return True
    else:
        return False

async def verify_zs_nd_code(zsro_gdf, sro_gdf):
    missing_mask = zsro_gdf['zs_nd_code'].isna() | (zsro_gdf['zs_nd_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_nd_code' est vide dans la table ZSRO")

    valid_codes = set(sro_gdf['nd_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_nd_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_nd_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zn_r3_code' non compatible avec 'nd_code' de la table SRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_zn_code(zsro_gdf, znro_gdf):
    missing_mask = zsro_gdf['zs_zn_code'].isna() | (zsro_gdf['zs_zn_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_zn_code' est vide dans la table ZSRO")

    valid_codes = set(znro_gdf['zn_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_zn_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_zn_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_zn_code' non compatible avec 'zn_code' de la table ZNRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_r1_code(zsro_gdf, znro_gdf):
    missing_mask = zsro_gdf['zs_r1_code'].isna() | (zsro_gdf['zs_r1_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_r1_code' est vide dans la table ZSRO")

    valid_codes = set(znro_gdf['zn_r1_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_r1_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_r1_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_r1_code' non compatible avec 'zn_r1_code' de la table ZNRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_r2_code(zsro_gdf, znro_gdf):
    missing_mask = zsro_gdf['zs_r2_code'].isna() | (zsro_gdf['zs_r2_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_r2_code' est vide dans la table ZSRO")

    valid_codes = set(znro_gdf['zn_r2_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_r2_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_r2_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_r2_code' non compatible avec 'zn_r2_code' de la table ZNRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_r3_code(zsro_gdf, znro_gdf):
    missing_mask = zsro_gdf['zs_r3_code'].isna() | (zsro_gdf['zs_r3_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_r3_code' est vide dans la table ZSRO")

    valid_codes = set(znro_gdf['zn_r3_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_r3_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_r3_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_r3_code' non compatible avec 'zn_r3_code' de la table ZNRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_r4_code(zsro_gdf, sro_gdf):
    missing_mask = zsro_gdf['zs_r4_code'].isna() | (zsro_gdf['zs_r4_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_r4_code' est vide dans la table ZSRO")
    valid_codes = set(sro_gdf['nd_r4_code'].dropna().unique())
    mismatch_mask = (~zsro_gdf['zs_r4_code'].isin(valid_codes)) & (~missing_mask)  
    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_r4_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_r4_code' non compatible avec 'zn_r4_code' de la table SRO:")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_refpm(zsro_gdf):
    missing_zs_refpm = zsro_gdf['zs_refpm'].isna() | (zsro_gdf['zs_refpm'] == '')
    
    if missing_zs_refpm.any():
        print("La colonne 'zs_refpm' n'est pas remplie dans la table attributaire de ZSRO")
        return True
    else:
        return False

async def verify_zs_capamax(zsro_gdf):
    invalid_zs_capamax = []
    valid_values = [576, 600, 720, 800, 864]
    
    for i, row in zsro_gdf.iterrows():
        zs_capamax = row['zs_capamax']
        zs_r4_code = row['zs_r4_code']
        
        if pd.isna(zs_capamax):
            print(f"ZSRO has zs_capamax which is missing.")
            invalid_zs_capamax.append(zs_r4_code)
        elif zs_capamax not in valid_values:
            print(f"ZSRO {zs_r4_code} has zs_capamax {zs_capamax} which is not among the valid values {valid_values}.")
            invalid_zs_capamax.append(zs_r4_code)
    
    return invalid_zs_capamax

async def verify_pcn_ftth(zpa_gdf, pb_gdf, zpbo_gdf, zsro_gdf, adresse_gdf):
    async def check_table(gdf, table_name):
        missing_pcn_ftth = gdf['pcn_ftth'].isna() | (gdf['pcn_ftth'] == '')
        
        if missing_pcn_ftth.any():
            print(f"La colonne pcn_ftth contient une/des valeurs manquantes dans la table {table_name}.")
            return False
        
        invalid_pcn_ftth = []
        for i, row in gdf.iterrows():
            geometry = row['geometry']
            za_pcn_ftth = row['pcn_ftth']
            
            adresse_within = adresse_gdf[adresse_gdf.within(geometry)]
            aggregate_pcn_ftth = adresse_within['pcn_ftth'].sum()
            
            if za_pcn_ftth != aggregate_pcn_ftth:
                invalid_pcn_ftth.append(row['pcn_code'])
                print(f"{table_name} {row['pcn_code']} a pcn_ftth {za_pcn_ftth} qui ne correspond pas au nombre d'EL: {aggregate_pcn_ftth}.")
        
        return invalid_pcn_ftth

    # Vérification pour chaque table
    invalid_zpa = check_table(zpa_gdf, 'ZPA')
    invalid_pb = check_table(pb_gdf, 'PB')
    invalid_zpbo = check_table(zpbo_gdf, 'ZPBO')
    invalid_zsro = check_table(zsro_gdf, 'ZSRO')

    return {
        'invalid_zpa': invalid_zpa,
        'invalid_pb': invalid_pb,
        'invalid_zpbo': invalid_zpbo,
        'invalid_zsro': invalid_zsro
    }

async def verify_pcn_ftte_zsro(zsro_gdf, adresse_gdf):
    missing_pcn_ftte = zsro_gdf['pcn_ftte'].isna() | (zsro_gdf['pcn_ftte'] == '')
    
    if missing_pcn_ftte.any():
        print("La colonne pcn_ftte n'est pas remplie dans la table attributaire de ZSRO")
        return False
    
    invalid_pcn_ftte = []
    for i, row in zsro_gdf.iterrows():
        zs_geometry = row['geometry']
        zs_pcn_ftte = row['pcn_ftte']
        
        adresse_within_zsro = adresse_gdf[adresse_gdf.within(zs_geometry)]
        aggregate_pcn_ftte = adresse_within_zsro['pcn_ftte'].sum()
        
        if zs_pcn_ftte != aggregate_pcn_ftte:
            invalid_pcn_ftte.append(row['zs_code'])
            print(f"ZSRO {row['zs_code']} a pcn_ftth {zs_pcn_ftte} qui ne correspond pas à la valeur correcte: {aggregate_pcn_ftte}.")
    
    return invalid_pcn_ftte

async def verify_pcn_umtot_zsro(zsro_gdf, pb_gdf):
    missing_pcn_umtot = zsro_gdf['pcn_umtot'].isna() | (zsro_gdf['pcn_umtot'] == '')
    
    if missing_pcn_umtot.any():
        print("La colonne pcn_umtot n'est pas remplie dans la table attributaire de ZSRO")
        return False
    
    invalid_pcn_umtot = []
    for i, row in zsro_gdf.iterrows():
        zs_geometry = row['geometry']
        zs_pcn_umtot = row['pcn_umtot']
        
        pb_within_zsro = pb_gdf[pb_gdf.within(zs_geometry)]
        correct_pcn_umtot = pb_within_zsro['pcn_umftth'].sum()
        
        if zs_pcn_umtot != correct_pcn_umtot:
            invalid_pcn_umtot.append(row['zs_code'])
            print(f"ZSRO {row['zs_code']} a pcn_ftth {zs_pcn_umtot} qui ne correspond pas à la valeur correcte: {correct_pcn_umtot}.")
    
    return invalid_pcn_umtot
