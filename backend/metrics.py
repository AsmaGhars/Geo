from prometheus_client import Counter, Gauge, Info

ANOMALY_COUNT = Gauge('anomaly_count', 'Nombre d’anomalies détectées lors de la dernière exécution')
DUPLICATE_COUNT = Gauge('duplicate_count', 'Nombre de doublons détectés', ['file_key', 'column_name', 'duplicate_value'])

NOT_IN_ZONE = Gauge(
    'not_in_zone',
    'Indicator that a geometry fell outside its assigned zone (0 or 1)',
    ['zone_type', 'code']
)

LONG_CONN_PERCENT = Gauge(
    'long_connections_percentage',
    'Pourcentage de raccordements RA dont la longueur > 90 m'
)
LONG_CONN_CM_LENGTH = Gauge(
    'long_connections_cm_length',
    'Longueur des raccordements CM dépassant 500 mètres',
    ['cm_codeext']
)
CB_CAPAFO_EXCESS = Gauge(
    'cb_aerial_capacity_excess',
    'Capacité des câbles aériens dépassant 144 FO',
    ['cl_codeext']
)
PA_UMFTTH_EXCESS = Gauge(
    'pa_umftth_excess',
    'Valeur du µm FTTH dépassant 20 µm par PA',
    ['pcn_code']
)
SRO_UMTOT_EXCESS = Gauge(
    'sro_umtot_excess',
    'Valeur du µm TOTALE dépassant 90 µm par PM',
    ['zs_code']
)
CB_D1_LENGTH_EXCESS = Gauge(
    'cb_d1_length_excess',
    'Longueur des CB D1 dépassant 2100 mètres',
    ['cl_codeext']
)
PA_ON_ENEDIS_SUPPORT = Gauge(
    "pa_on_enedis_support",
    "PA superposé sur appui ENEDIS",
    ["pcn_code"]
)
ZPBO_NOT_IN_ZPA = Gauge(
    'zpbo_not_in_zpa',
    'Indicator that a zpbo fell outside its assigned zpa',
    ['zone_type', 'code']
)
SUPPORT_DISTANCE_EXCEEDING_MAX = Gauge(
    'support_distance_exceeding_max',
    'Distance between supports exceeding the maximum allowed distance',
    ['start_support_code', 'end_support_code']
)
ZPA_NOT_IN_ZSRO = Gauge(
    'zpa_not_in_zsro',
    'Indicator that a zpa fell outside its assigned zsro',
    ['zone_type', 'code']
)
ZSRO_NOT_IN_ZNRO = Gauge(
    'zsro_not_in_znro',
    'Indicator that a zsro fell outside its assigned znro',
    ['zone_type', 'code']
)
PBR_EL_EXCESS = Gauge(
    'pbr_el_excess',
    'PBRs associés à plus de 3 EL',
    ['pcn_code']
)
PB_SINGLE_EL = Gauge(
    'pb_single_el',
    'PBs à 1 EL',
    ['pcn_code']
)
INVALID_PCN_CODE_PA = Gauge(
    'invalid_pcn_code_pa',
    'Invalid pcn_code in PA table',
    ['pcn_code', 'expected_pcn_code']
)
MISSING_PCN_CODE_PA = Gauge(
    'missing_pcn_code_pa',
    'Nombre de lignes PA avec pcn_code manquant'
)
INVALID_PCN_CB_ENT_PA = Gauge(
    'invalid_pcn_cb_ent_pa',
    'Invalid pcn_cb_ent in PA table',
    ['pcn_code', 'pcn_cb_ent_pa', 'expected_pcn_cb_ent_pa']
)
MISSING_PCN_CB_ENT_PA = Gauge(
    'missing_pcn_cb_ent_pa',
    'Nombre de lignes PA avec pcn_cb_ent_pa manquant'
)
