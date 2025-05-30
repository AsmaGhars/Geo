import geopandas as gpd
from shapely.geometry import  LineString, MultiPolygon, Polygon
from shapely import wkt
import pandas as pd
import re, os
from ..metrics import *

def reset_metrics():
    print("Resetting all metrics...") 
    ANOMALY_COUNT.set(0)
    MISSING_PCN_CODE_PA.set(0)
    MISSING_PCN_CB_ENT_PA.set(0)
    DUPLICATE_COUNT.clear()
    NOT_IN_ZONE.clear()
    LONG_CONN_PERCENT.set(0)
    LONG_CONN_CM_LENGTH.clear()
    CB_CAPAFO_EXCESS.clear()
    PA_UMFTTH_EXCESS.clear()
    SRO_UMTOT_EXCESS.clear()
    CB_D1_LENGTH_EXCESS.clear()
    PA_ON_ENEDIS_SUPPORT.clear()
    ZPBO_NOT_IN_ZPA.clear()
    SUPPORT_DISTANCE_EXCEEDING_MAX.clear()
    ZPA_NOT_IN_ZSRO.clear()
    ZSRO_NOT_IN_ZNRO.clear()
    PBR_EL_EXCESS.clear()
    PB_SINGLE_EL.clear()
    INVALID_PCN_CODE_PA.clear()
    INVALID_PCN_CB_ENT_PA.clear()
    print("done")

def convert_geometries_to_wkt(gdf):
    gdf['wkt'] = gdf['geometry'].apply(lambda geom: wkt.dumps(geom))
    return gdf

async def verify_cb_capafo(cb_gdf, support_gdf):
    filtered_supports = support_gdf[support_gdf['pcn_newsup'].str.contains("POTEAU|IMMEUBLE", case=False, na=False)]
    
    invalid_cb_capafo = []

    for i, cb_row in cb_gdf.iterrows():
        has_support = any(cb_row.geometry.intersects(support.geometry) for support in filtered_supports.itertuples())
        
        if has_support and cb_row['cb_capafo'] > 144:
            invalid_cb_capafo.append(cb_row['cl_codeext'])
            CB_CAPAFO_EXCESS.labels(cl_codeext=cb_row['cl_codeext']).set(cb_row['cb_capafo'])

    if invalid_cb_capafo:
        ANOMALY_COUNT.inc(len(invalid_cb_capafo))
        print("Les CB aeriens doivent avoir une capacité max de 144 FO:")
        for cb in invalid_cb_capafo:
            print(f"- {cb}")
    
    return invalid_cb_capafo

async def verify_mic_pa(zpa_gdf):

    invalid_pas = []
    pa_invalid = zpa_gdf[zpa_gdf['pcn_umftth'] > 20]
    
    for _, row in pa_invalid.iterrows():
        code = row['pcn_code']
        um_value = row['pcn_umftth']
        PA_UMFTTH_EXCESS.labels(pcn_code=code).set(um_value)
        invalid_pas.append((code, um_value))
    if invalid_pas:
        ANOMALY_COUNT.inc(len(invalid_pas))
        print("PAs dépassant 20 µm FTTH par PA :")
        for code, um_value in invalid_pas:
            print(f"- {code} : {um_value} µm")
    
    return invalid_pas

async def verify_long_connections(cm_di_gdf):
    try:
        long_connections = cm_di_gdf[(cm_di_gdf['cm_long'] > 90) & (cm_di_gdf['cm_typelog'] == 'RA')]
        total_connections = len(cm_di_gdf)
        long_connections_count = len(long_connections)
        long_connections_percentage = (long_connections_count / total_connections) * 100
        LONG_CONN_PERCENT.set(long_connections_percentage)
        if long_connections_percentage > 5:
            print(f"Le pourcentage de raccordements longs dépasse 5%: {long_connections_percentage:.2f}%")
            ANOMALY_COUNT.inc(1)
        long_connections_cm = cm_di_gdf[cm_di_gdf['cm_long'] > 500]
        if len(long_connections_cm) > 0:
            ANOMALY_COUNT.inc(len(long_connections_cm))
            print(f"{len(long_connections_cm)} raccordement(s) CM dépasse(nt) 500 mètres:")
            for _, row in long_connections_cm.iterrows():
                code = row['cm_codeext']
                longueur = row['cm_long']
                LONG_CONN_CM_LENGTH.labels(cm_codeext=code).set(longueur)
                print(f" • {code} : {longueur:.1f} m")
    except Exception as e:
        print(f"Erreur lors de la vérification des raccordements longs : {e}")

async def verify_length_D1(cb_di_gdf):
    invalid_cbs = []
    try:
        cb_filtered = cb_di_gdf[cb_di_gdf['cl_codeext'].str.contains('D1', na=False)]
        matching_cb = cb_filtered[cb_filtered['cb_long'] > 2100]

        for _, row in matching_cb.iterrows():
            code = row['cl_codeext']
            length = row['cb_long']
            CB_D1_LENGTH_EXCESS.labels(cl_codeext=code).set(length)
            invalid_cbs.append((code, length))

        if invalid_cbs:
            ANOMALY_COUNT.inc(len(invalid_cbs))
            print("CB D1 dépassant 2100 m :")
            for code, length in invalid_cbs:
                print(f"- {code} : {length} m")

    except Exception as e:
        print(f"Erreur lors de la vérification des longueurs : {e}")

    return invalid_cbs

async def verify_no_overlap(pa_gdf: gpd.GeoDataFrame, support_gdf: gpd.GeoDataFrame):
    invalid = []
    rows_to_export = []

    try:
        enedis = support_gdf[support_gdf['pt_prop'].str.upper() == 'ENEDIS']
        overlaps = gpd.overlay(pa_gdf, enedis, how='intersection')

        for _, row in overlaps.iterrows():
            code = row['pcn_code']
            PA_ON_ENEDIS_SUPPORT.labels(pcn_code=code).set(1)
            invalid.append(code)
            rows_to_export.append(row)

        if invalid:
            ANOMALY_COUNT.inc(len(invalid))
            print(f"Interdit d'avoir des PA sur des appuis ENEDIS : {invalid}")

            # Exportation de la couche des anomalies
            export_gdf = gpd.GeoDataFrame(rows_to_export, crs=pa_gdf.crs)

            downloads = os.path.join(os.path.expanduser("~"), "Downloads")
            os.makedirs(downloads, exist_ok=True)

            shp_path = os.path.join(downloads, "pa_on_enedis_support.shp")
            gpkg_path = os.path.join(downloads, "pa_on_enedis_support.gpkg")

            export_gdf.to_file(shp_path, driver='ESRI Shapefile', encoding='UTF-8')
            print(f"Shapefile exporté dans : {shp_path}")

            export_gdf.to_file(gpkg_path, layer="pa_on_enedis_support", driver='GPKG')
            print(f"GeoPackage exporté dans : {gpkg_path}")

    except Exception as e:
        print(f"Erreur lors de la vérification de la superposition : {e}")

    return invalid

async def verify_zpb_in_zonepa(zpbo_gdf, zpa_gdf):
    if zpbo_gdf.crs != zpa_gdf.crs:
        zpbo_gdf = zpbo_gdf.to_crs(zpa_gdf.crs)
    zpa_gdf = convert_geometries_to_wkt(zpa_gdf)
    zpa_dict = {row['pcn_code']: wkt.loads(row['wkt']) for _, row in zpa_gdf.iterrows()}

    records = []
    for _, row in zpbo_gdf.iterrows():
        geom = row['geometry'] if 'geometry' in row else wkt.loads(row['wkt'])
        code = row['pcn_code']
        zpa_geom = zpa_dict.get(row['pcn_zpa'])
        if zpa_geom is None:
            continue
        diff = geom.difference(zpa_geom)
        if diff.is_empty:
            continue
        parts = []
        if isinstance(diff, Polygon):
            parts = [diff]
        elif isinstance(diff, MultiPolygon):
            parts = list(diff.geoms)
        for part in parts:
            records.append({'pcn_code': code, 'wkt': part.wkt})
            ZPBO_NOT_IN_ZPA.labels(zone_type='ZPA', code=code).set(1)
    if not records:
        print("Aucune partie de ZPBO n’est hors de ZPA.")
        return []
    export_gdf = gpd.GeoDataFrame([
        {'pcn_code': r['pcn_code'], 'geometry': wkt.loads(r['wkt'])} for r in records
    ], crs=zpbo_gdf.crs)
    downloads = os.path.join(os.path.expanduser("~"), "Downloads")
    os.makedirs(downloads, exist_ok=True)
    export_gdf.to_file(os.path.join(downloads, "zpb_not_in_zpa.shp"), driver='ESRI Shapefile', encoding='UTF-8', engine='fiona')
    export_gdf.to_file(os.path.join(downloads, "zpb_not_in_zpa.gpkg"), layer="zpb_not_in_zpa", driver='GPKG')
    ANOMALY_COUNT.inc(len(records))
    return records

async def verify_max_distance_between_supports(cm_gdf, support_gdf, max_distance=40):
    if cm_gdf.crs != support_gdf.crs:
        support_gdf = support_gdf.to_crs(cm_gdf.crs)

    filtered_support_gdf = support_gdf[support_gdf['pcn_newsup'].str.startswith('POTEAU')]

    support_distances_exceeding_max = []

    for cm_idx, cm_row in cm_gdf.iterrows():
        cm_line = cm_row.geometry
        support_points = []

        for support_idx, support_row in filtered_support_gdf.iterrows():
            support_point = support_row.geometry
            if cm_line.intersects(support_point):
                support_points.append((support_point, support_row['pt_codeext']))

        support_points.sort(key=lambda point: cm_line.project(point[0]))

        for i in range(len(support_points) - 1):
            start_point, start_support_code = support_points[i]
            end_point, end_support_code = support_points[i + 1]
            distance = start_point.distance(end_point)

            segment = LineString([start_point, end_point])
            other_supports_between = support_gdf[~support_gdf['pcn_newsup'].str.startswith('POTEAU') & 
                                                 support_gdf.intersects(segment)]

            if other_supports_between.empty:
                if distance > max_distance:
                    support_distances_exceeding_max.append((start_support_code, end_support_code, distance))
                    SUPPORT_DISTANCE_EXCEEDING_MAX.labels(start_support_code=start_support_code, end_support_code=end_support_code).set(distance)
                    print(f"La distance entre les supports {start_support_code} et {end_support_code} dépasse {max_distance} mètres : {distance:.2f} mètres")

    if support_distances_exceeding_max:
        ANOMALY_COUNT.inc(len(support_distances_exceeding_max))

    return support_distances_exceeding_max

async def verify_zpa_in_zonesro(zpa_gdf, zsro_gdf):
    if zpa_gdf.crs != zsro_gdf.crs:
        zpa_gdf = zpa_gdf.to_crs(zsro_gdf.crs)

    zsro_geom = zsro_gdf.iloc[0]['geometry']
    records = []

    for _, row in zpa_gdf.iterrows():
        zpa_geom = row['geometry']
        code = row['pcn_code']

        # Calculer la différence géométrique : partie de la ZPA hors ZSRO
        diff = zpa_geom.difference(zsro_geom)

        if diff.is_empty:
            continue

        parts = []
        if isinstance(diff, Polygon):
            parts = [diff]
        elif isinstance(diff, MultiPolygon):
            parts = list(diff.geoms)

        for part in parts:
            records.append({'pcn_code': code, 'wkt': part.wkt})
            ZPA_NOT_IN_ZSRO.labels(zone_type='ZSRO', code=code).set(1)

    if not records:
        print("Toutes les ZPA sont entièrement contenues dans la ZSRO.")
        return []

    # Créer une GeoDataFrame pour les parties en dehors
    export_gdf = gpd.GeoDataFrame([
        {'pcn_code': r['pcn_code'], 'geometry': wkt.loads(r['wkt'])} for r in records
    ], crs=zpa_gdf.crs)

    # Sauvegarder les fichiers dans le dossier Téléchargements
    downloads = os.path.join(os.path.expanduser("~"), "Downloads")
    os.makedirs(downloads, exist_ok=True)

    export_gdf.to_file(os.path.join(downloads, "zpa_not_in_zsro.shp"), driver='ESRI Shapefile', encoding='UTF-8', engine='fiona')
    export_gdf.to_file(os.path.join(downloads, "zpa_not_in_zsro.gpkg"), layer="zpa_not_in_zsro", driver='GPKG')

    ANOMALY_COUNT.inc(len(records))
    return records

async def verify_PBR_EL(pb_gdf):
    filtered_pbs = pb_gdf[pb_gdf['pcn_pbtyp'].str.contains("PBR", case=False, na=False)]
    invalid_pbr = []
    for i, row in filtered_pbs.iterrows():
        if row['pcn_ftth'] > 3:
            invalid_pbr.append(row['pcn_code'])
            PBR_EL_EXCESS.labels(pcn_code=row['pcn_code']).set(row['pcn_ftth'])
    if invalid_pbr:
        ANOMALY_COUNT.inc(len(invalid_pbr))
        print("Les PBRs doivent être associés max à 3EL:")
        for pbr in invalid_pbr:
            print(f"- {pbr}")
    return invalid_pbr

async def singleEL(pb_gdf):
    try:
        if 'pcn_ftth' in pb_gdf.columns and 'pcn_code' in pb_gdf.columns:
            pb_filtered = pb_gdf[pb_gdf['pcn_ftth'] == 1]
            if not pb_filtered.empty:
                pcn_codes = pb_filtered['pcn_code'].tolist()
                for code in pcn_codes:
                    PB_SINGLE_EL.labels(pcn_code=code).set(1)
                ANOMALY_COUNT.inc(len(pcn_codes))
                print(f"PB(s) à 1EL détecté(s) pcn_code: {pcn_codes}")
        else:
            print(f"Les colonnes nécessaires 'pcn_ftth' et 'pcn_code' ne sont pas présentes dans le Shapefile.")
    except Exception as e:
        print(f"Erreur lors de la vérification des pb à 1EL dans le shapefile : {e}")

#Table attributaire du PA

async def verify_pcn_code_pa(pa_gdf, zpa_gdf):
    missing_pcn_codes = []
    invalid_pcn_codes = []

    for i, row in pa_gdf.iterrows():
        pa_geometry = row['geometry']
        pa_pcn_code = row['pcn_code']

        if pd.isna(pa_pcn_code) or pa_pcn_code == '':
            print(f"Ligne {i} : Valeur manquante dans la colonne 'pcn_code'")
            missing_pcn_codes.append(i)
            MISSING_PCN_CODE_PA.inc() 
            ANOMALY_COUNT.inc()
            continue

        zpa_intersections = zpa_gdf[zpa_gdf.intersects(pa_geometry)]
        intersecting_pcn_codes = zpa_intersections['pcn_code'].dropna().tolist()

        if pa_pcn_code not in intersecting_pcn_codes:
            corrected_pcn_code = intersecting_pcn_codes[0] if intersecting_pcn_codes else 'N/A'
            print(f"Ligne {i} : 'pcn_code' incorrect : {pa_pcn_code}, attendu dans {intersecting_pcn_codes}")
            invalid_pcn_codes.append(i)
            INVALID_PCN_CODE_PA.labels(pcn_code=pa_pcn_code, expected_pcn_code=corrected_pcn_code).set(1)
            ANOMALY_COUNT.inc()

    return {
        "missing_pcn_codes": missing_pcn_codes,
        "invalid_pcn_codes": invalid_pcn_codes
    }

async def verify_pcn_cb_ent_pa(pa_gdf, cb_di_gdf):
    missing_pcn_cb_ent = pa_gdf['pcn_cb_ent'].isna() | (pa_gdf['pcn_cb_ent'] == '')
    num_missing_pcn_cb_ent = missing_pcn_cb_ent.sum()
    
    if num_missing_pcn_cb_ent > 0:
        print(f"La colonne pcn_cb_ent contient {num_missing_pcn_cb_ent} valeurs manquantes dans la table attributaire de PA")
        MISSING_PCN_CB_ENT_PA.inc(num_missing_pcn_cb_ent)
        ANOMALY_COUNT.inc(num_missing_pcn_cb_ent)
    
    invalid_pcn_cb_ent = []

    for i, row in pa_gdf.iterrows():
        pa_geometry = row['geometry']
        pa_pcn_cb_ent_ex = row['pcn_cb_ent']
        
        def intersects_with_pa(x):
            boundary = x.geometry.boundary
            if boundary.geom_type == 'MultiPoint':
                return any(pa_geometry.contains(point) for point in boundary.geoms)
            elif boundary.geom_type == 'Point':
                return pa_geometry.contains(boundary)
            else:
                return False
        
        cb_intersections = cb_di_gdf[cb_di_gdf.apply(intersects_with_pa, axis=1)]
        
        if cb_intersections.empty:
            print(f"No intersections found for PA {row['pcn_code']}.")
            continue
        
        cb_capafo_values = cb_intersections['cb_capafo'].values
        max_cb_capafo = max(cb_capafo_values)
        
        if pa_pcn_cb_ent_ex != max_cb_capafo:
            invalid_pcn_cb_ent.append(row['pcn_code'])
            INVALID_PCN_CB_ENT_PA.labels(pcn_code=row['pcn_code'], pcn_cb_ent_pa=pa_pcn_cb_ent_ex, expected_pcn_cb_ent_pa=max_cb_capafo).set(1)
            ANOMALY_COUNT.inc()
            print(f"PA {row['pcn_code']} has pcn_cb_ent : {pa_pcn_cb_ent_ex} which does not match the maximum cb_capafo value: {max_cb_capafo}.")
    
    return invalid_pcn_cb_ent

#Table attributaire du ZPA

def verify_pcn_code_zpa(zpa_gdf):
    missing_pcn_code = zpa_gdf['pcn_code'].isna() | (zpa_gdf['pcn_code'] == '')
    
    if missing_pcn_code.any():
        print("La colonne pcn_code contient une/des valeurs manquantes dans la table attributaire de ZPA")
        return False
    invalid_pcn_code = []
    pattern = re.compile(r'^([A-Za-z]{3}_[A-Za-z0-9]{5})_PA_\d{5}$')
    
    first_pcn_code = zpa_gdf.iloc[0]['pcn_code']
    first_part = first_pcn_code.split('_PA_')[0]
    
    parts = []
    
    for i, row in zpa_gdf.iterrows():
        pcn_code = row['pcn_code']
        match = pattern.match(pcn_code)
        
        if not match:
            invalid_pcn_code.append(pcn_code)
            print(f"pcn_code de PA {pcn_code} ne correspond pas à la bonne structure.")
        else:
            current_part = pcn_code.split('_PA_')[0]
            parts.append(current_part)
    
    if len(set(parts)) > 1:
        print("Il y a des pcn_code qui ne correspondent pas à la bonne structure.")
        invalid_pcn_code.extend([pcn_code for pcn_code, part in zip(zpa_gdf['pcn_code'], parts) if part != first_part])
    
    return invalid_pcn_code

def verify_pcn_capa_zpa(zpa_gdf, cb_di_gdf):
    missing_pcn_capa = zpa_gdf['pcn_capa'].isna() | (zpa_gdf['pcn_capa'] == '')
    
    if missing_pcn_capa.any():
        print("La colonne pcn_capa contient une/des valeurs manquantes dans la table attributaire de ZPA")
        return False
    invalid_pcn_capa = []

    for i, row in zpa_gdf.iterrows():
        zpa_geometry = row['geometry']
        zpa_pcn_capa = row['pcn_capa']
        def intersects_with_zpa(x):
            boundary = x.geometry.boundary
            if boundary.geom_type == 'MultiPoint':
                return any(zpa_geometry.contains(point) for point in boundary.geoms)
            elif boundary.geom_type == 'Point':
                return zpa_geometry.contains(boundary)
            else:
                return False
        
        cb_intersections = cb_di_gdf[cb_di_gdf.apply(intersects_with_zpa, axis=1)]
        
        if cb_intersections.empty:
            print(f"No intersections found for ZPA {row['pcn_code']}.")
            continue
        
        cb_capafo_values = cb_intersections['cb_capafo'].values
        
        max_cb_capafo = max(cb_capafo_values)
        
        if zpa_pcn_capa != max_cb_capafo:
            invalid_pcn_capa.append(row['pcn_code'])
            print(f"ZPA {row['pcn_code']} has pcn_capa {zpa_pcn_capa} which does not match the maximum cb_capafo value {max_cb_capafo}.")
    
    return invalid_pcn_capa

def verify_pcn_ftth_zpa(zpa_gdf, adresse_gdf):
    missing_pcn_ftth = zpa_gdf['pcn_ftth'].isna() | (zpa_gdf['pcn_ftth'] == '')
    
    if missing_pcn_ftth.any():
        print("La colonne pcn_ftth contient une/des valeurs manquantes dans la table attributaire de ZPA")
        return False
    
    invalid_pcn_ftth = []
    for i, row in zpa_gdf.iterrows():
        za_geometry = row['geometry']
        za_pcn_ftth = row['pcn_ftth']
        
        adresse_within_zpa = adresse_gdf[adresse_gdf.within(za_geometry)]
        aggregate_pcn_ftth = adresse_within_zpa['pcn_ftth'].sum()
        
        if za_pcn_ftth != aggregate_pcn_ftth:
            invalid_pcn_ftth.append(row['pcn_code'])
            print(f"ZPA {row['pcn_code']} a pcn_ftth {za_pcn_ftth} qui ne correspond pas au nombre d'EL: {aggregate_pcn_ftth}.")
    
    return invalid_pcn_ftth

def verify_pcn_umftth_zpa(zpa_gdf, pb_gdf):
    missing_pcn_umftth = zpa_gdf['pcn_umftth'].isna() | (zpa_gdf['pcn_umftth'] == '')
    
    if missing_pcn_umftth.any():
        print("La colonne pcn_umftth contient une/des valeurs manquantes dans la table attributaire de ZPA")
        return False
    
    invalid_pcn_umftth = []
    for i, row in zpa_gdf.iterrows():
        za_geometry = row['geometry']
        za_pcn_umftth = row['pcn_umftth']
        
        pb_within_zpa = pb_gdf[pb_gdf.within(za_geometry)]
        aggregate_pcn_umftth = pb_within_zpa['pcn_umftth'].sum()
        
        if za_pcn_umftth != aggregate_pcn_umftth:
            invalid_pcn_umftth.append(row['pcn_code'])
            print(f"ZPA {row['pcn_code']} a pcn_umftth {za_pcn_umftth} qui ne correspond pas au nombre d'EL: {aggregate_pcn_umftth}.")
    
    return invalid_pcn_umftth

def verify_pcn_ftte_zpa(zpa_gdf, adresse_gdf):
    missing_pcn_ftte = zpa_gdf['pcn_ftte'].isna() | (zpa_gdf['pcn_ftte'] == '')
    
    if missing_pcn_ftte.any():
        print("La colonne pcn_ftte contient une/des valeurs manquantes dans la table attributaire de ZPA")
        return False
    
    invalid_pcn_ftte = []
    for i, row in zpa_gdf.iterrows():
        za_geometry = row['geometry']
        za_pcn_ftte = row['pcn_ftte']
        
        adresse_within_zpa = adresse_gdf[adresse_gdf.within(za_geometry)]
        aggregate_pcn_ftte = adresse_within_zpa['pcn_ftte'].sum()
        
        if za_pcn_ftte != aggregate_pcn_ftte:
            invalid_pcn_ftte.append(row['pcn_code'])
            print(f"ZPA {row['pcn_code']} a pcn_ftth {za_pcn_ftte} qui ne correspond pas au nombre d'EL: {aggregate_pcn_ftte}.")
    
    return invalid_pcn_ftte

def verify_pcn_umftte_zpa(zpa_gdf, pb_gdf):
    if 'pcn_umftte' not in pb_gdf.columns:
        print("La colonne 'pcn_umftte' n'existe pas dans pb_gdf.")
        return False
    missing_pcn_umftte = zpa_gdf['pcn_umftte'].isna() | (zpa_gdf['pcn_umftte'] == '')
    
    if missing_pcn_umftte.any():
        print("La colonne pcn_umftte contient une/des valeurs manquantes dans la table attributaire de ZPA")
        return False
    invalid_pcn_umftte = []
    for i, row in zpa_gdf.iterrows():
        za_geometry = row['geometry']
        za_pcn_umftte = row['pcn_umftte']
        
        pb_within_zpa = pb_gdf[pb_gdf.within(za_geometry)]
        aggregate_pcn_umftte = pb_within_zpa['pcn_umftte'].sum()
        
        if za_pcn_umftte != aggregate_pcn_umftte:
            invalid_pcn_umftte.append(row['pcn_code'])
            print(f"ZPA {row['pcn_code']} a pcn_umftte {za_pcn_umftte} qui ne correspond pas au nombre d'EL: {aggregate_pcn_umftte}.")
    
    return invalid_pcn_umftte

def verify_pcn_umuti_zpa(zpa_gdf):
    missing_pcn_umuti = zpa_gdf['pcn_umuti'].isna() | (zpa_gdf['pcn_umuti'] == '')
    
    if missing_pcn_umuti.any():
        print("La colonne pcn_umuti contient une/des valeurs manquantes dans la table attributaire de ZPA:")
        missing_codes = zpa_gdf[missing_pcn_umuti]['pcn_code']
        print(missing_codes.tolist())
        return False
    
    invalid_pcn_umuti = []
    for i, row in zpa_gdf.iterrows():
        za_pcn_umuti = row['pcn_umuti']
        aggregate_pcn_umuti = row['pcn_umftth'] + row['pcn_umftte']
        
        if za_pcn_umuti != aggregate_pcn_umuti:
            invalid_pcn_umuti.append(row['pcn_code'])
            print(f"ZPA {row['pcn_code']} a pcn_umuti {za_pcn_umuti} qui ne correspond pas à la somme : pcn_umftth + pcn_umftte: {aggregate_pcn_umuti}.")
    
    return invalid_pcn_umuti

def verify_pcn_umrsv_zpa(zpa_gdf):
    missing_pcn_umrsv = zpa_gdf['pcn_umrsv'].isna() | (zpa_gdf['pcn_umrsv'] == '')
    
    if missing_pcn_umrsv.any():
        print("La colonne pcn_umrsv contient une/des valeurs manquantes dans la table attributaire de ZPA:")
        missing_codes = zpa_gdf[missing_pcn_umrsv]['pcn_code']
        print("Codes pcn manquants :")
        print(missing_codes.tolist())
        return False
    
    invalid_pcn_umrsv = []
    for i, row in zpa_gdf.iterrows():
        za_pcn_umrsv = row['pcn_umrsv']
        aggregate_pcn_umrsv = (row['pcn_capa'] / 6) - row['pcn_umuti']
        
        if za_pcn_umrsv != aggregate_pcn_umrsv:
            invalid_pcn_umrsv.append(row['pcn_code'])
            print(f"ZPA {row['pcn_code']} a pcn_umrsv {za_pcn_umrsv} qui ne correspond pas à la formule (pcn_capa / 6) - pcn_umuti: {aggregate_pcn_umrsv}.")
    
    return invalid_pcn_umrsv

def verify_pcn_umtot_zpa(zpa_gdf):
    missing_pcn_umtot = zpa_gdf['pcn_umtot'].isna() | (zpa_gdf['pcn_umtot'] == '')
    
    if missing_pcn_umtot.any():
        print("La colonne pcn_umtot contient une/des valeurs manquantes dans la table attributaire de ZPA:")
        missing_codes = zpa_gdf[missing_pcn_umtot]['pcn_code']
        print(missing_codes.tolist())
        return False
    
    invalid_pcn_umtot = []
    for i, row in zpa_gdf.iterrows():
        za_pcn_umtot = row['pcn_umtot']
        aggregate_pcn_umtot = row['pcn_umuti'] + row['pcn_umrsv']
        
        if za_pcn_umtot != aggregate_pcn_umtot:
            invalid_pcn_umtot.append(row['pcn_code'])
            print(f"ZPA {row['pcn_code']} a pcn_umtot {za_pcn_umtot} qui ne correspond pas à la somme : pcn_umuti + pcn_umrsv: {aggregate_pcn_umtot}.")
    
    return invalid_pcn_umtot

def verify_pcn_sro(zpa_gdf, zsro_gdf, type):
    missing_mask = zpa_gdf['pcn_sro'].isna() | (zpa_gdf['pcn_sro'] == '') 
    if missing_mask.any():
        print(f"La colonne 'pcn_sro' contient des champs vides dans la table {type}")
        return False

    valid_codes = set(zsro_gdf['zs_r4_code'].dropna().unique())

    mismatch_mask = (~zpa_gdf['pcn_sro'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zpa_gdf.loc[mismatch_mask, 'pcn_sro'].tolist()

    if invalid_codes:
        print(f"{type} a un (des) 'pcn_sro' incorrecte(s) :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

#Table attributaire du PB

def verify_PB_pcn_umftth(pb_gdf):
    invalid_pb = []
    for i, row in pb_gdf.iterrows():
        pcn_pbtyp = row['pcn_pbtyp']
        pcn_umftth = row['pcn_umftth']
        pcn_code = row['pcn_code']
        pcn_ftth = row['pcn_ftth']
        if pcn_pbtyp == 'PBR6e' and pcn_umftth != 0:
            invalid_pb.append(pcn_code)
        elif pcn_pbtyp in ['PB6', 'PBR6m', 'PBR12e', 'PBR12m'] and pcn_umftth != 1:
            invalid_pb.append(pcn_code)
        elif pcn_pbtyp == 'PB12' and pcn_umftth != 2:
            invalid_pb.append(pcn_code)
        elif pcn_pbtyp == 'PBI' and ((pcn_ftth>5 and pcn_umftth != 2) or (pcn_ftth<=5 and pcn_umftth!=1)) :
            invalid_pb.append(pcn_code)
    if invalid_pb:
        print("Les PBs suivants ont des valeurs incorrectes pour pcn_umftth:")
        for pbr in invalid_pb:
            print(f"- {pbr}")
    return invalid_pb

def verify_PB_pcn_pbtyp(pb_gdf):
    missing_mask = pb_gdf['pcn_pbtyp'].isna() | (pb_gdf['pcn_pbtyp'] == '') 
    if missing_mask.any():
        print(f"La colonne 'pcn_pbtyp' contient des champs vides dans la table PB")
        return False
    invalid_pb = []
    valid_pb = ["PB6", "PBR6e", "PBR6m", "PB12", "PBR12e", "PBR12m", "PBI"]
    for i, row in pb_gdf.iterrows():
        pcn_pbtyp = row['pcn_pbtyp']
        pcn_code = row['pcn_code']
        if pcn_pbtyp not in valid_pb:
            invalid_pb.append(pcn_code)
    if invalid_pb:
        print("pcn_pbtyp invalides pour ces PB:")
        for pb in invalid_pb:
            print(f"- {pb}")
    return invalid_pb

def verify_pcn_ftth_pb(pb_gdf, zpbo_gdf):
    missing_mask = pb_gdf['pcn_ftth'].isna() | (pb_gdf['pcn_ftth'] == '') 
    if missing_mask.any():
        print(f"La colonne 'pcn_ftth' contient des champs vides dans la table PB")
        return False
    invalid_pcn_ftth = []

    for i, row in pb_gdf.iterrows():
        pcn_ftth_point = row['geometry']
        pcn_ftth_value = row['pcn_ftth']
        pcn_code = row['pcn_code']
        
        within_zpbo = zpbo_gdf[zpbo_gdf.geometry.contains(pcn_ftth_point)]
        
        if not within_zpbo.empty:
            first_value = within_zpbo.iloc[0]['pcn_ftth']
            
            if pcn_ftth_value != first_value:
                invalid_pcn_ftth.append(pcn_code)
                print(f"PCN {pcn_code} has pcn_ftth {pcn_ftth_value} which does not match the overlay value {first_value}.")
        else:
            print(f"PCN {pcn_code} is not within any ZPBO geometry.")
    return invalid_pcn_ftth

def verify_pcn_code_pb(pb_gdf):
    missing_pcn_code = pb_gdf['pcn_code'].isna() | (pb_gdf['pcn_code'] == '')
    
    if missing_pcn_code.any():
        print("La colonne pcn_code contient une/des valeurs manquantes dans la table attributaire de PB")
        return False
    invalid_pcn_code = []
    pattern = re.compile(r'^([A-Za-z]{3}_[A-Za-z0-9]{5})_PB_\d{5}$')
    
    first_pcn_code = pb_gdf.iloc[0]['pcn_code']
    first_part = first_pcn_code.split('_PB_')[0]
    
    parts = []
    
    for i, row in pb_gdf.iterrows():
        pcn_code = row['pcn_code']
        match = pattern.match(pcn_code)
        
        if not match:
            invalid_pcn_code.append(pcn_code)
            print(f"pcn_code de PB {pcn_code} ne correspond pas à la bonne structure.")
        else:
            current_part = pcn_code.split('_PB_')[0]
            parts.append(current_part)
    
    if len(set(parts)) > 1:
        print("Il y a des pcn_code qui ne correspondent pas à la bonne structure.")
        invalid_pcn_code.extend([pcn_code for pcn_code, part in zip(pb_gdf['pcn_code'], parts) if part != first_part])
    
    return invalid_pcn_code

def verify_pcn_zpa(pb_gdf, zpa_gdf):
    invalid_pcn_zpa = []
    for i, row in pb_gdf.iterrows():
        pb_geometry = row['geometry']
        pcn_zpa_value = row['pcn_zpa']
        pcn_code = row['pcn_code']
        intersects_zpa = zpa_gdf[zpa_gdf.geometry.intersects(pb_geometry)]
        if not intersects_zpa.empty:
            intersecting_codes = intersects_zpa['pcn_code'].tolist()
            intersecting_string = ', '.join(intersecting_codes)
            if pcn_zpa_value != intersecting_string:
                invalid_pcn_zpa.append(pcn_code)
        else:
            print(f"PCN {pcn_code} does not intersect with any ZPA geometry.")
    if invalid_pcn_zpa:
        print("Les PCNs suivants ont des valeurs incorrectes pour pcn_zpa:")
        for pcn in invalid_pcn_zpa:
            print(f"- {pcn}")
    return invalid_pcn_zpa

def verify_pcn_commen_pb(pb_gdf):
    invalid_pcn_commen = []
    filtered_pbs = pb_gdf[pb_gdf['pcn_pbtyp'].isin(['PBR6e', 'PBR12e'])]
    for i, row in filtered_pbs.iterrows():
        pcn_commen_value = row['pcn_commen']
        pcn_code = row['pcn_code']
        if pd.isna(pcn_commen_value) or pcn_commen_value == '':
            invalid_pcn_commen.append(pcn_code)
            print(f"PCN {pcn_code} has pcn_pbtyp {row['pcn_pbtyp']} but pcn_commen is missing or empty.")
        else:
            if pcn_commen_value not in pb_gdf['pcn_code'].values:
                invalid_pcn_commen.append(pcn_code)
                print(f"PCN {pcn_code} has pcn_pbtyp {row['pcn_pbtyp']} but pcn_commen refers to a non-existing PBR master {pcn_commen_value}.")
    return invalid_pcn_commen

def verify_pcn_rac_lg_pb(pb_gdf, cb_di_gdf):
    invalid_pcn_rac_lg = []

    for i, pb_row in pb_gdf.iterrows():
        pb_geometry = pb_row['geometry']
        pcn_rac_lg_value = pb_row['pcn_rac_lg']
        pcn_code = pb_row['pcn_code']
        
        filtered_cb_di = cb_di_gdf[(cb_di_gdf['cb_long'] > 90) & (cb_di_gdf['cb_typelog'] == 'RA') & (cb_di_gdf.geometry.intersects(pb_geometry))]
        
        count_long_connections = filtered_cb_di.shape[0]
        
        if pcn_rac_lg_value != count_long_connections:
            invalid_pcn_rac_lg.append(pcn_code)
            print(f"PCN {pcn_code} has pcn_rac_lg {pcn_rac_lg_value} which does not match the count of long connections {count_long_connections}.")

    if invalid_pcn_rac_lg:
        print("Les PCNs suivants ont des valeurs incorrectes pour pcn_rac_lg:")
        for pcn in invalid_pcn_rac_lg:
            print(f"- {pcn}")
    
    return invalid_pcn_rac_lg

def verify_pcn_cb_ent_pb(pb_gdf):
    invalid_pcn_cb_ent = []
    
    for i, row in pb_gdf.iterrows():
        pcn_pbtyp = row['pcn_pbtyp']
        pcn_ftth = row['pcn_ftth']
        pcn_cb_ent = row['pcn_cb_ent']
        pcn_code = row['pcn_code']
                
        if pcn_pbtyp in ['PBR6e']:
            expected_pcn_cb_ent = 2 if pcn_ftth == 2 else 4
        elif pcn_pbtyp in ['PBR6m']:
            expected_pcn_cb_ent = 2 if pcn_ftth == 2 else pcn_ftth
        elif pcn_pbtyp in ['PBR12e', 'PBR12m']:
            expected_pcn_cb_ent = 6
        elif 1 <= pcn_ftth <= 5:
            expected_pcn_cb_ent = 6
        elif 6 <= pcn_ftth <= 10:
            expected_pcn_cb_ent = 12
        elif 11 <= pcn_ftth <= 20:
            expected_pcn_cb_ent = 24
        elif 21 <= pcn_ftth <= 30:
            expected_pcn_cb_ent = 36
        elif 31 <= pcn_ftth <= 60:
            expected_pcn_cb_ent = 72
        else:
            expected_pcn_cb_ent = None
                
        if pcn_cb_ent != expected_pcn_cb_ent:
            invalid_pcn_cb_ent.append(pcn_code)
            print(f"Invalid pcn_cb_ent for pcn_code={pcn_code}: {pcn_cb_ent} != {expected_pcn_cb_ent}")
    
    if invalid_pcn_cb_ent:
        print("Les PCNs suivants ont des valeurs incorrectes pour pcn_cb_ent:")
        for pcn in invalid_pcn_cb_ent:
            print(f"- {pcn}")
    
    return invalid_pcn_cb_ent

# Le tableau attributaitre de ZPBO

def verify_pcn_code_zpbo(zpbo_gdf, pb_gdf):
    missing_pcn_code = zpbo_gdf['pcn_code'].isna() | (zpbo_gdf['pcn_code'] == '')
    if missing_pcn_code.any():
        print("La colonne pcn_code contient des valeurs manquantes dans la table attributaire de ZPBO")
        return False
    
    invalid_pcn_code = []

    for i, row in zpbo_gdf.iterrows():
        zpbo_geometry = row['geometry']
        zpbo_pcn_code = row['pcn_code']
        
        pb_intersections = pb_gdf[pb_gdf.intersects(zpbo_geometry)]
        
        intersecting_pcn_codes = pb_intersections['pcn_code'].tolist()
        
        expected_pcn_code = ', '.join(intersecting_pcn_codes)
        
        if zpbo_pcn_code != expected_pcn_code:
            invalid_pcn_code.append(row['pcn_code'])
            print(f"ZPBO {expected_pcn_code} a pcn_code incorrecte: {row['pcn_code']}")
    
    return invalid_pcn_code

def verify_zp_r4_code(zpbo_gdf, zsro_gdf):
    missing_mask = zpbo_gdf['zp_r4_code'].isna() | (zpbo_gdf['zp_r4_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zp_r4_code' est vide dans la table ZSRO")
    valid_codes = set(zsro_gdf['zs_r4_code'].dropna().unique())
    mismatch_mask = (~zpbo_gdf['zp_r4_code'].isin(valid_codes)) & (~missing_mask)  
    invalid_codes = zpbo_gdf.loc[mismatch_mask, 'zp_r4_code'].tolist()

    if invalid_codes:
        print("La ZPBO aun (des) 'zp_r4_code' incorrecte(s):")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

def verify_pcn_zpa_zpbo(zpbo_gdf, zpa_gdf):
    invalid_pcn_zpa = []
    for i, row in zpbo_gdf.iterrows():
        zpbo_geometry = row['geometry']
        pcn_zpa_value = row['pcn_zpa']
        pcn_code = row['pcn_code']
        intersects_zpa = zpa_gdf[zpa_gdf.geometry.intersects(zpbo_geometry)]
        if not intersects_zpa.empty:
            intersecting_codes = intersects_zpa['pcn_code'].tolist()
            intersecting_string = ', '.join(intersecting_codes)
            if pcn_zpa_value != intersecting_string:
                invalid_pcn_zpa.append(pcn_code)
        else:
            print(f"PCN {pcn_code} does not intersect with any ZPA geometry.")
    if invalid_pcn_zpa:
        print("Les PCNs suivants ont des valeurs incorrectes pour pcn_zpa:")
        for pcn in invalid_pcn_zpa:
            print(f"- {pcn}")
    return invalid_pcn_zpa
