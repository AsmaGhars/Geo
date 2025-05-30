from flask import Blueprint, request, jsonify
import os, yaml, requests, aiofiles
from backend.config import TEMP_DIR
from backend.scripts.extract_zip import extract_zip
from backend.scripts.load_data import load_data
from backend.find_shapefiles import find_shapefiles
from backend.scripts.verify_di import *
from backend.scripts.verify import *
from ..metrics import *

ALERTMANAGER_CONFIG_PATH = r"C:/prometheus/alertmanager.yml"
ALERTMANAGER_RELOAD_URL = "http://localhost:9093/-/reload"  

upload_blueprint = Blueprint('upload', __name__)
@upload_blueprint.route('/save-email', methods=['POST'])
def save_email():
    data = request.get_json()
    email = data.get('email')

    try:
        with open(ALERTMANAGER_CONFIG_PATH, 'r') as file:
            config = yaml.safe_load(file)

        for receiver in config.get('receivers', []):
            if receiver.get('name') == 'team-email':
                receiver['email_configs'][0]['to'] = email

        with open(ALERTMANAGER_CONFIG_PATH, 'w') as file:
            yaml.safe_dump(config, file)

        reload_response = requests.post(ALERTMANAGER_RELOAD_URL)
        if reload_response.status_code != 200:
            print(f"Erreur lors du reload Alertmanager: {reload_response.text}")
            return jsonify({"status": "error", "message": "Reload failed"}), 500

        return jsonify({"status": "success", "updated_email": email}), 200

    except Exception as e:
        print(f"Erreur lors de save-email : {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
@upload_blueprint.route('/upload', methods=['POST'])
async def upload_file():
    reset_metrics()
    if 'file' not in request.files or 'choice' not in request.form or 'email' not in request.form or 'message' not in request.form:
        return jsonify({"error": "Missing file, choice, email, or message part"}), 400
    
    file = request.files['file']
    choice = request.form['choice']
    email = request.form['email']
    message = request.form['message']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    if not file.filename.endswith('.zip'):
        return jsonify({"error": "Please upload a ZIP file"}), 400

    file_path = os.path.join(TEMP_DIR, file.filename)
    async with aiofiles.open(file_path, 'wb') as f:
        await f.write(file.read())
    
    info_path = os.path.join(TEMP_DIR, f"{file.filename}.txt")
    async with aiofiles.open(info_path, 'w') as info_file:
        await info_file.write(f"Email: {email}\nMessage: {message}\n")

    extract_to = TEMP_DIR
    zip_path = file_path
    
    await extract_zip(zip_path, extract_to)
    
    shapefiles = await find_shapefiles(extract_to)

    if choice == 'di':
        required_shapefiles = [
            'PB.shp', 'PA.shp', 'ZPBO.shp', 'ZSRO.shp', 'ZNRO.shp', 'ZPA.shp',
            'CB_DI.shp', 'ADRESSE.shp', 'CM_DI.shp', 'SUPPORT.shp', 'SRO.shp',
            'NRO.shp', 'PEP_DI.shp', 'CREATION_CONDUITE_DI.shp'
        ]
    elif choice == 'tr':
        required_shapefiles = [
            'PB.shp', 'PA.shp', 'ZPBO.shp', 'ZSRO.shp', 'ZNRO.shp', 'ZPA.shp',
            'CB_TR.shp', 'ADRESSE.shp', 'CM_TR.shp', 'SUPPORT.shp', 'SRO.shp',
            'NRO.shp', 'PEP_TR.shp', 'CREATION_CONDUITE_TR.shp'
        ]
    else:
        return jsonify({"error": "Invalid choice"}), 400
    
    found_shapefiles = []
    for client, files in shapefiles.items():
        found_shapefiles.extend(files)
    
    missing_shapefiles = [shp for shp in required_shapefiles if not any(shp in file for file in found_shapefiles)]
    
    if missing_shapefiles:
        return jsonify({"error": f"Missing shapefiles: {missing_shapefiles}"}), 400
        
    pb_path = next(file for file in found_shapefiles if 'PB.shp' in file)
    pa_path = next(file for file in found_shapefiles if 'PA.shp' in file)
    zpbo_path = next(file for file in found_shapefiles if 'ZPBO.shp' in file)
    zsro_path = next(file for file in found_shapefiles if 'ZSRO.shp' in file)
    znro_path = next(file for file in found_shapefiles if 'ZNRO.shp' in file)
    zpa_path = next(file for file in found_shapefiles if 'ZPA.shp' in file)
    cb_path = next(file for file in found_shapefiles if f'CB_{choice.upper()}.shp' in file)
    adresse_path = next(file for file in found_shapefiles if 'ADRESSE.shp' in file)
    cm_path = next(file for file in found_shapefiles if f'CM_{choice.upper()}.shp' in file)
    support_path = next(file for file in found_shapefiles if 'SUPPORT.shp' in file)
    sro_path = next(file for file in found_shapefiles if 'SRO.shp' in file)
    nro_path = next(file for file in found_shapefiles if 'NRO.shp' in file)
    pep_path = next(file for file in found_shapefiles if f'PEP_{choice.upper()}.shp' in file)
    creation_conduite_path = next(file for file in found_shapefiles if f'CREATION_CONDUITE_{choice.upper()}.shp' in file)
    
    pb_gdf, zpbo_gdf, zsro_gdf, zpa_gdf, cb_gdf, pa_gdf, znro_gdf, adresse_gdf, cm_gdf, support_gdf, sro_gdf, nro_gdf, pep_gdf, creation_conduite_gdf = await load_data(
        pb_path, zpbo_path, zsro_path, zpa_path, cb_path, pa_path, znro_path, adresse_path, cm_path, support_path, sro_path, nro_path, pep_path, creation_conduite_path
    )
    
    
    if choice == 'di':
        invalid_PBR_EL = await verify_PBR_EL(pb_gdf)
        invalid_cb_capafo = await verify_cb_capafo(cb_gdf, support_gdf)
        dataframes = [
                ("CB_DI", cb_gdf),
                ("CM_DI", cm_gdf),
                ("PB", pb_gdf),
                ("ADRESSE", adresse_gdf),
                ("NRO", nro_gdf),
                ("PA", pa_gdf),
                ("PEP", pep_gdf),
                ("SRO", sro_gdf),
                ("SUPPORT", support_gdf),
                ("ZNRO", znro_gdf),
                ("ZPA", zpa_gdf),
                ("ZPBO", zpbo_gdf),
                ("ZSRO", zsro_gdf),
            ]
        invalid_duplicates = await check_duplicates(dataframes)
        invalid_singleEL = await singleEL(pb_gdf)
        invalid_mic_pm = await verify_mic_pm(zsro_gdf)
        invalid_mic_pa = await verify_mic_pa(zpa_gdf)
        invalid_long_connections = await verify_long_connections(cm_gdf)
        invalid_length_D1 = await verify_length_D1(cb_gdf)
        invalid_no_overlap = await verify_no_overlap(pa_gdf, support_gdf)
        invalid_cb_intersections = await verify_c_intersections(cb_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf, adresse_gdf, 'CB')
        invalid_cm_intersections = await verify_c_intersections(cm_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf, adresse_gdf, 'CM')
        invalid_self_intersections_cb = await detect_self_intersections_c(cb_gdf, 'CB')
        invalid_self_intersections_cm = await detect_self_intersections_c(cm_gdf, 'CM')
        not_in_zones_pa, nd_code_mismatch_pa = await verify_geometries_in_zones(pa_gdf, zpa_gdf, 'PA')
        not_in_zones_pb, nd_code_mismatch_pb = await verify_geometries_in_zones(pb_gdf, zpbo_gdf, 'PB')
        not_in_zones_sro, nd_code_mismatch_sro = await verify_geometries_in_zones(sro_gdf, zsro_gdf, 'SRO')
        not_in_zones_nro, nd_code_mismatch_nro = await verify_geometries_in_zones(nro_gdf, znro_gdf, 'NRO')
        invalid_zpb_in_zonepa = await verify_zpb_in_zonepa(zpbo_gdf, zpa_gdf)
        invalid_max_distance_between_supports = await verify_max_distance_between_supports(cm_gdf, support_gdf)
        invalid_zsro_in_zonenro = await verify_zsro_in_zonenro(zsro_gdf, znro_gdf)
        invalid_zpa_in_zonesro = await verify_zpa_in_zonesro(zpa_gdf, zsro_gdf)
        invalid_zpbo_intersections = await check_zp_intersections(zpbo_gdf, 'PB')
        invalid_zpa_intersections = await check_zp_intersections(zpa_gdf, 'PA')
        invalid_cb_without_cm = await detect_cb_without_cm(cb_gdf, cm_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf)
        incorrect_direction_cables = await verify_cable_direction(cb_gdf, nro_gdf, sro_gdf, pa_gdf, pb_gdf, adresse_gdf)
        # invalid_pcn_ftth_pb = verify_pcn_ftth_pb(pb_gdf, zpbo_gdf)
        # invalid_nd_code = verify_nd_code(nro_gdf, 'NRO')
        # invalid_nd_code = verify_nd_code(sro_gdf, 'SRO')
        # invalid_zn_code = verify_zn_code(znro_gdf)
        # invalid_nd_r3_code= verify_nd_r3_code(nro_gdf)
        # invalid_zn_nd_code = verify_zn_nd_code(znro_gdf, nro_gdf)
        # invalid_zn_r1_code= verify_zn_r1_code(znro_gdf)
        # invalid_zn_r2_code= verify_zn_r2_code(znro_gdf)
        # invalid_zn_r3_code = verify_zn_r3_code(znro_gdf, nro_gdf)
        # invalid_zn_nroref =  verify_zn_nroref(znro_gdf)
        # invalid_pcn_cb_ent_sro = verify_pcn_cb_ent_sro(sro_gdf, adresse_gdf)
        # invalid_nd_r4_code = verify_nd_r4_code(sro_gdf)
        # invalid_zs_code = verify_zs_code(zsro_gdf)
        # invalid_zs_nd_code = verify_zs_nd_code(zsro_gdf, sro_gdf)
        # invalid_zs_zn_code = verify_zs_zn_code(zsro_gdf, znro_gdf)
        # invalid_zs_r1_code = verify_zs_r1_code(zsro_gdf, znro_gdf)
        # invalid_zs_r2_code = verify_zs_r2_code(zsro_gdf, znro_gdf)
        # invalid_zs_r3_code = verify_zs_r3_code(zsro_gdf, znro_gdf)
        # invalid_zs_r4_code = verify_zs_r4_code(zsro_gdf, sro_gdf)
        # invalid_zs_refpm = verify_zs_refpm(zsro_gdf)
        # invalid_zs_capamax = verify_zs_capamax(zsro_gdf)
        # invalid_pcn_ftte_zsro = verify_pcn_ftte_zsro(zsro_gdf, adresse_gdf)
        # invalid_pcn_umtot_zsro = verify_pcn_umtot_zsro(zsro_gdf, pb_gdf)
        # invalid_pcn_code_zpa = await verify_pcn_code_zpa(zpa_gdf)
        # invalid_pcn_capa_zpa = verify_pcn_capa_zpa(zpa_gdf, cb_gdf)
        # invalid_pcn_ftth_zpa = verify_pcn_ftth_zpa(zpa_gdf, adresse_gdf)
        # invalid_pcn_umftth_zpa = verify_pcn_umftth_zpa(zpa_gdf, pb_gdf)
        # invalid_pcn_ftte_zpa = verify_pcn_ftte_zpa(zpa_gdf, adresse_gdf)
        # invalid_pcn_umftte_zpa = verify_pcn_umftte_zpa(zpa_gdf, pb_gdf)
        # invalid_pcn_umuti_zpa = verify_pcn_umuti_zpa(zpa_gdf)
        # invalid_pcn_umrsv_zpa = verify_pcn_umrsv_zpa(zpa_gdf)
        # invalid_pcn_umtot_zpa = verify_pcn_umtot_zpa(zpa_gdf)
        # invalid_pcn_sro_zpa = verify_pcn_sro(zpa_gdf, zsro_gdf, 'ZPA')
        # invalid_pcn_sro_pa = verify_pcn_sro(pa_gdf, zsro_gdf, 'PA')
        invalid_pcn_code_pa = await verify_pcn_code_pa(pa_gdf, zpa_gdf)
        invalid_pcn_cb_ent_pa = await verify_pcn_cb_ent_pa(pa_gdf, cb_gdf)
        # invalid_pcn_code_pb = verify_pcn_code_pb(pb_gdf)
        # invalid_PB_pcn_pbtyp= verify_PB_pcn_pbtyp(pb_gdf)
        # invalid_pcn_ftth = verify_pcn_ftth(zpa_gdf, pb_gdf, zpbo_gdf, zsro_gdf, adresse_gdf)
        # invalid_PB_pcn_umftth = verify_PB_pcn_umftth(pb_gdf)
        # invalid_pcn_sro_pb = verify_pcn_sro(pb_gdf, zsro_gdf, 'PB')
        # invalid_pcn_zpa = verify_pcn_zpa(pb_gdf, zpa_gdf)
        # invalid_pcn_cb_ent_pb = verify_pcn_cb_ent_pb(pb_gdf)
        # invalid_pcn_commen_pb = verify_pcn_commen_pb(pb_gdf)
        # invalid_pcn_rac_lg_pb = verify_pcn_rac_lg_pb(pb_gdf, cb_gdf)
        # invalid_pcn_code_zpbo = verify_pcn_code_zpbo(zpbo_gdf, pb_gdf)
        # invalid_zp_r4_code = verify_zp_r4_code(zpbo_gdf, zsro_gdf)
        # invalid_pcn_zpa_zpbo = verify_pcn_zpa_zpbo(zpbo_gdf, zpa_gdf)
        return jsonify({
            "Invalid PBR EL": invalid_PBR_EL,
            "invalid_cb_capafo": invalid_cb_capafo,
            "invalid_duplicates": invalid_duplicates,
            "invalid_singleEL": invalid_singleEL,
            "invalid_mic_pm": invalid_mic_pm,
            "invalid_mic_pa": invalid_mic_pa,
            "invalid_long_connections": invalid_long_connections,
            "invalid_length_D1": invalid_length_D1,
            "invalid_no_overlap": invalid_no_overlap,
            "invalid_cb_intersections": invalid_cb_intersections,
            "invalid_cm_intersections": invalid_cm_intersections,
            "invalid_self_intersections_cb": invalid_self_intersections_cb,
            "invalid_self_intersections_cm": invalid_self_intersections_cm,
            "Not in zones PA": not_in_zones_pa,
            "ND code mismatch PA": nd_code_mismatch_pa,
            "Not in zones PB": not_in_zones_pb,
            "ND code mismatch PB": nd_code_mismatch_pb,
            "Not in zones SRO": not_in_zones_sro,
            "ND code mismatch SRO": nd_code_mismatch_sro,
            "Not in zones NRO": not_in_zones_nro,
            "ND code mismatch NRO": nd_code_mismatch_nro,
            "invalid_zpb_in_zonepa": invalid_zpb_in_zonepa,
            "invalid_max_distance_between_supports": invalid_max_distance_between_supports,
            "invalid_zsro_in_zonenro": invalid_zsro_in_zonenro,
            "invalid_zpa_in_zonesro": invalid_zpa_in_zonesro,
            "invalid_zpbo_intersections": invalid_zpbo_intersections,
            "invalid_zpa_intersections": invalid_zpa_intersections,
            "invalid_cb_without_cm": invalid_cb_without_cm,
            "incorrect_direction_cables": incorrect_direction_cables,
            # "invalid_pcn_ftth_pb": invalid_pcn_ftth_pb,
            # "invalid_nd_code": invalid_nd_code,
            # "invalid_nd_r3_code": invalid_nd_r3_code,
            # "invalid_zn_code": invalid_zn_code,
            # "invalid_zn_nd_code": invalid_zn_nd_code,
            # "invalid_zn_r1_code": invalid_zn_r1_code,
            # "invalid_zn_r2_code": invalid_zn_r2_code,
            # "invalid_zn_r3_code": invalid_zn_r3_code,
            # "invalid_zn_nroref": invalid_zn_nroref,
            # "invalid_pcn_cb_ent_sro": invalid_pcn_cb_ent_sro,
            # "invalid_nd_r4_code": invalid_nd_r4_code,
            # "invalid_zs_code": invalid_zs_code,
            # "invalid_zs_nd_code": invalid_zs_nd_code,
            # "invalid_zs_zn_code": invalid_zs_zn_code,
            # "invalid_zs_r1_code": invalid_zs_r1_code,
            # "invalid_zs_r2_code": invalid_zs_r2_code,
            # "invalid_zs_r3_code": invalid_zs_r3_code,
            # "invalid_zs_r4_code": invalid_zs_r4_code,
            # "invalid_zs_refpm": invalid_zs_refpm,
            # "invalid_zs_capamax": invalid_zs_capamax,
            # "invalid_pcn_ftte_zsro": invalid_pcn_ftte_zsro,
            # "invalid_pcn_umtot_zsro": invalid_pcn_umtot_zsro,
            # "invalid_pcn_ftth_zpa": invalid_pcn_ftth_zpa,
            # "invalid_pcn_umftth_zpa": invalid_pcn_umftth_zpa,
            # "invalid_pcn_ftte_zpa": invalid_pcn_ftte_zpa,
            # "invalid_pcn_umftte_zpa": invalid_pcn_umftte_zpa,
            # "invalid_pcn_umuti_zpa": invalid_pcn_umuti_zpa,
            # "invalid_pcn_umrsv_zpa": invalid_pcn_umrsv_zpa,
            # "invalid_pcn_umtot_zpa": invalid_pcn_umtot_zpa,
            # "invalid_pcn_sro_zpa": invalid_pcn_sro_zpa,
            # "invalid_pcn_sro_pa": invalid_pcn_sro_pa,
            "invalid_pcn_code_pa": invalid_pcn_code_pa,
            "invalid_pcn_cb_ent_pa": invalid_pcn_cb_ent_pa,
            # "invalid_pcn_code_zpa": invalid_pcn_code_zpa,
            # "invalid_pcn_code_pb": invalid_pcn_code_pb,
            # "invalid_PB_pcn_pbtyp": invalid_PB_pcn_pbtyp,
            # "invalid_pcn_ftth": invalid_pcn_ftth,
            # "Invalid_PB_pcn_umftth": invalid_PB_pcn_umftth,
            # "invalid_pcn_sro_pb": invalid_pcn_sro_pb,
            # "invalid_pcn_zpa": invalid_pcn_zpa,
            # "invalid_pcn_cb_ent_pb": invalid_pcn_cb_ent_pb,
            # "invalid_pcn_commen_pb": invalid_pcn_commen_pb,
            # "invalid_pcn_rac_lg_pb": invalid_pcn_rac_lg_pb,
            # "invalid_pcn_code_zpbo": invalid_pcn_code_zpbo,
            # "invalid_zp_r4_code": invalid_zp_r4_code,
            # "invalid_pcn_zpa_zpbo": invalid_pcn_zpa_zpbo,
        })
    elif choice == 'tr':
        invalid_cb_capafo = verify_cb_capafo(cb_gdf, support_gdf)
        dataframes = [
                ("CB_TR", cb_gdf),
                ("CM_TR", cm_gdf),
                ("PB", pb_gdf),
                ("ADRESSE", adresse_gdf),
                ("NRO", nro_gdf),
                ("PA", pa_gdf),
                ("PEP", pep_gdf),
                ("SRO", sro_gdf),
                ("SUPPORT", support_gdf),
                ("ZNRO", znro_gdf),
                ("ZPA", zpa_gdf),
                ("ZPBO", zpbo_gdf),
                ("ZSRO", zsro_gdf),
            ]
        invalid_duplicates = check_duplicates(dataframes)
        invalid_mic_pm = await verify_mic_pm(zsro_gdf)
        # invalid_length_D1 = verify_length_D1(cb_gdf)
        invalid_cb_intersections = await verify_c_intersections(cb_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf, adresse_gdf, 'CB')
        invalid_cm_intersections = await verify_c_intersections(cm_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf, adresse_gdf, 'CM')
        invalid_self_intersections_cb = await detect_self_intersections_c(cb_gdf, 'CB')
        invalid_self_intersections_cm = await detect_self_intersections_c(cm_gdf, 'CM')
        not_in_zones_sro, nd_code_mismatch_sro = await verify_geometries_in_zones(sro_gdf, zsro_gdf, 'SRO')
        not_in_zones_nro, nd_code_mismatch_nro = await verify_geometries_in_zones(nro_gdf, znro_gdf, 'NRO')
        invalid_zsro_in_zonenro = await verify_zsro_in_zonenro(zsro_gdf, znro_gdf)
        invalid_zsro_intersections = await check_zp_intersections(zsro_gdf, 'SRO')
        invalid_cb_without_cm = await detect_cb_without_cm(cb_gdf, cm_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf)
        incorrect_direction_cables = await verify_cable_direction(cb_gdf, nro_gdf, sro_gdf, pa_gdf, pb_gdf, adresse_gdf)
        # invalid_nd_code= verify_nd_code(nro_gdf, 'NRO')
        # invalid_nd_code = verify_nd_code(sro_gdf, 'SRO')
        # invalid_zn_code = verify_zn_code(znro_gdf)
        # invalid_nd_r3_code= verify_nd_r3_code(nro_gdf)
        # invalid_zn_nd_code = verify_zn_nd_code(znro_gdf, nro_gdf)
        # invalid_zn_r1_code= verify_zn_r1_code(znro_gdf)
        # invalid_zn_r2_code= verify_zn_r2_code(znro_gdf)
        # invalid_zn_r3_code = verify_zn_r3_code(znro_gdf, nro_gdf)
        # invalid_zn_nroref =  verify_zn_nroref(znro_gdf)
        # invalid_pcn_cb_ent_sro = verify_pcn_cb_ent_sro(sro_gdf, adresse_gdf)
        # invalid_nd_r4_code = verify_nd_r4_code(sro_gdf)
        # invalid_zs_code = verify_zs_code(zsro_gdf)
        # invalid_zs_nd_code = verify_zs_nd_code(zsro_gdf, sro_gdf)
        # invalid_zs_zn_code = verify_zs_zn_code(zsro_gdf, znro_gdf)
        # invalid_zs_r1_code = verify_zs_r1_code(zsro_gdf, znro_gdf)
        # invalid_zs_r2_code = verify_zs_r2_code(zsro_gdf, znro_gdf)
        # invalid_zs_r3_code = verify_zs_r3_code(zsro_gdf, znro_gdf)
        # invalid_zs_r4_code = verify_zs_r4_code(zsro_gdf, sro_gdf)
        # invalid_zs_refpm = verify_zs_refpm(zsro_gdf)
        # invalid_zs_capamax = verify_zs_capamax(zsro_gdf)
        # invalid_pcn_ftte_zsro = verify_pcn_ftte_zsro(zsro_gdf, adresse_gdf)
        # invalid_pcn_umtot_zsro = verify_pcn_umtot_zsro(zsro_gdf, pb_gdf)
        # invalid_pcn_sro_zpa = verify_pcn_sro(zpa_gdf, zsro_gdf, 'ZPA')
        # invalid_pcn_sro_pa = verify_pcn_sro(pa_gdf, zsro_gdf, 'PA')
        # invalid_pcn_ftth = verify_pcn_ftth(zpa_gdf, pb_gdf, zpbo_gdf, zsro_gdf, adresse_gdf)
        # invalid_pcn_sro_pb = verify_pcn_sro(pb_gdf, zsro_gdf, 'PB')
        return jsonify({
            "invalid_duplicates": invalid_duplicates,
            "invalid_mic_pm": invalid_mic_pm,
            "invalid_cb_intersections": invalid_cb_intersections,
            "invalid_cm_intersections": invalid_cm_intersections,
            "invalid_self_intersections_cb": invalid_self_intersections_cb,
            "invalid_self_intersections_cm": invalid_self_intersections_cm,
            "Not in zones SRO": not_in_zones_sro,
            "ND code mismatch SRO": nd_code_mismatch_sro,
            "Not in zones NRO": not_in_zones_nro,
            "ND code mismatch NRO": nd_code_mismatch_nro,
            "invalid_zsro_in_zonenro": invalid_zsro_in_zonenro,
            "invalid_zsro_intersections": invalid_zsro_intersections,
            "invalid_cb_without_cm": invalid_cb_without_cm,
            "incorrect_direction_cables": incorrect_direction_cables,
            # "invalid_nd_code": invalid_nd_code,
            # "invalid_nd_r3_code": invalid_nd_r3_code,
            # "invalid_zn_code": invalid_zn_code,
            # "invalid_zn_nd_code": invalid_zn_nd_code,
            # "invalid_zn_r1_code": invalid_zn_r1_code,
            # "invalid_zn_r2_code": invalid_zn_r2_code,
            # "invalid_zn_r3_code": invalid_zn_r3_code,
            # "invalid_zn_nroref": invalid_zn_nroref,
            # "invalid_pcn_cb_ent_sro": invalid_pcn_cb_ent_sro,
            # "invalid_nd_r4_code": invalid_nd_r4_code,
            # "invalid_zs_code": invalid_zs_code,
            # "invalid_zs_nd_code": invalid_zs_nd_code,
            # "invalid_zs_zn_code": invalid_zs_zn_code,
            # "invalid_zs_r1_code": invalid_zs_r1_code,
            # "invalid_zs_r2_code": invalid_zs_r2_code,
            # "invalid_zs_r3_code": invalid_zs_r3_code,
            # "invalid_zs_r4_code": invalid_zs_r4_code,
            # "invalid_zs_refpm": invalid_zs_refpm,
            # "invalid_zs_capamax": invalid_zs_capamax,
            # "invalid_pcn_ftte_zsro": invalid_pcn_ftte_zsro,
            # "invalid_pcn_umtot_zsro": invalid_pcn_umtot_zsro,
            # "invalid_pcn_ftth": invalid_pcn_ftth,
        })
    else:
        return jsonify({"error": "Invalid choice"}), 400