import os
import queue

import psycopg2
from secr import pss,pss2
from threading import Thread

def conn_db():
    return connect_to_db('10.122.3.134', '5432', 'greenplum-dwh', "gpadmin", pss)

def conn_cb():
    return connect_to_db('10.122.0.41', '5432', 'dwh_cloudberrydb', "admin_user", pss2())

def connect_to_db(host, port, dbname, user, password):
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    return conn


pgsql_keywords = {
    "CREATE", "DROP", "ALTER", "TABLE", "VIEW", "SCHEMA", "INDEX", "SEQUENCE",
    "MATERIALIZED VIEW", "PARTITION BY", "DISTRIBUTED BY", "EXTERNAL",
    "SELECT", "INSERT", "UPDATE", "DELETE", "COPY",
    "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE SAVEPOINT",
    "BEGIN", "END", "EXCEPTION", "RAISE", "LOOP", "WHILE", "FOR", "IF", "THEN", "ELSE", "ELSIF",
    "SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "REAL", "DOUBLE PRECISION",
    "CHAR", "VARCHAR", "TEXT", "BOOLEAN",
    "DATE", "TIME", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL",
    "BYTEA", "UUID", "JSON", "JSONB", "ARRAY", "XML", "CITEXT", "INET",
    "PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "NOT NULL", "CHECK", "DEFAULT",
    "NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
    "LOCALTIME", "LOCALTIMESTAMP",
    "COUNT", "SUM", "AVG", "MIN", "MAX", "ARRAY_AGG", "STRING_AGG",
    "RANK", "ROW_NUMBER", "DENSE_RANK", "LEAD", "LAG",
    "AND", "OR", "NOT", "LIKE", "ILIKE", "IN", "BETWEEN", "IS NULL", "IS NOT NULL",
    "ANY", "ALL", "EXISTS", "UNIQUE",
    "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN", "CROSS JOIN",
    "NATURAL JOIN", "ON", "USING",
    "WITH", "AS", "CASE", "WHEN", "THEN", "ELSE", "END",
    "DISTINCT", "GROUP BY", "ORDER BY", "HAVING", "LIMIT", "OFFSET",
    "UNION", "INTERSECT", "EXCEPT", "FETCH FIRST", "FOR UPDATE", "LOCK",
    "ANALYZE", "EXPLAIN", "VACUUM", "REINDEX",
    "BTREE", "HASH", "GIN", "BRIN", "SPGIST", "GIST",
    "APPENDONLY", "COLUMNSTORE", "ORC", "PARQUET",
    "DISTRIBUTED RANDOMLY", "SEGMENT REJECT LIMIT", "LOG ERRORS INTO",
    "CTID", "OID", "TABLESPACE", "WINDOW", "FILTER", "OVER",
    "GENERATOR", "UNLOGGED", "TEMPORARY", "PARALLEL",
}

table_names = [
    # 'ods__fido.transacts',
    # 'datamart.soft_collection_queue_log',
    # 'dictionary.regions_ru',
    # 'ods__avo_marketplace.merchants',
    'ods__avo_operation_manager.transactions',
    # 'datamart.contract_classification',
    # 'ods__tranzaxis.avo_tran',
    # 'datamart.avo_clients_offer_daily',
    # 'ods__fido.accounts_history',
    # 'ods__avo_users.app_users',
    # 'ods__wings.reports',
    # 'ods__tranzaxis.avo_token',
    # 'datamart.dmr_matrix',
    # 'ods__tranzaxis_cod.avo_account',
    # 'datamart_risks.application',
    # 'datamart_cod.accounts',
    # 'dictionary.atm_point_info',
    # 'ods__crm.service_requests',
    # 'ods__tranzaxis.avo_card_product',
    # 'ods__odata_1c.expenses',
    # 'datamart.wings_cl_events',
    # 'ods__crm.template_answers',
    # 'dictionary.mobile_operator',
    # 'datamart.portfolio_daily',
    # 'ods__tranzaxis.avo_entry',
    # 'ods__odata_1c.cfo',
    # 'ods__tranzaxis.avo_oper_day',
    # 'ods__fido.client_current',
    # 'ods__ktc.cash_unit_statuses',
    # 'datamart.contracts',
    # 'ods__tranzaxis.avo_token_product',
    # 'datamart.accounts',
    # 'ods__tranzaxis.avo_creditline_min_pay_delta',
    # #'temp.impressions',
    # 'ods__avo_registries.registries',
    # 'ods__fido_cod.day_operational',
    # 'ods__avo.currency_rates',
    # 'ods__odata_1c.expenses_dict',
    # 'ods__fido.dep_contracts',
    # 'ods__odata_1c.employee_personal_data',
    # 'ods__avo_message_storage.messages',
    # 'ods__fido.dep_accounts',
    # 'datamart.tx_fido_oper_day',
    # 'stg__appsflyer.impressions',
    # 'ods__wings.guides',
    # 'ods__odata_1c.currencies',
    # 'datamart.avo_app_install',
    # 'datamart.katm_no_nesting_data',
    # 'ods__fido.saldo',
    # 'datamart.logger_user_device_map',
    # 'ods__avo_marketplace.services',
    # 'ods__odata_1c.accounts',
    # 'ods__asterisk.agent_queues',
    # 'ods__asterisk.auto_dial_outbound_campaign',
    # 'ods__appsflyer.inapps',
    # 'ods__ktc.historical_cash_unit_status_p',
    # 'ods__odata_1c.counterparties',
    # 'ods__fido.transacts_history',
    # 'datamart.contract_classification_transactor_tech',
    # 'dictionary.mobile_notification_type',
    # 'dictionary.media_group',
    # 'dictionary.calendar',
    # 'dictionary.attraction_channel',
    # 'dictionary.io_fraud',
    # 'dictionary.mobile_code',
    # 'ods__tranzaxis.avo_account_plan_item',
    # 'ods__wings.temp_users_checked',
    # 'datamart.clients',
    # 'ods__wings.reports_request',
    # 'ods__asterisk.agents',
    # 'ods__avo_operation_manager.operations',
    # 'ods__avo_marketplace.categories',
    # 'datamart.fido_turnover',
    # 'datamart.avobank_ma_monthly',
    # 'dictionary.contract_class',
    # 'datamart_cod.fido_turnover',
    # 'ods__asterisk.auto_dial_outbound_campaign_data_set',
    # 'datamart_cod.tx_fido_oper_day',
    # 'datamart.avo_clients_offer_oper',
    # 'ods__avo_attempts.attempt_history',
    # 'datamart.avo_users_kyc_oper',
    # 'dictionary.bonus_offers',
    # 'dictionary.mobile_standart_notification',
    # 'ods__wings.cases',
    # 'ods__avo_offers.cl_transaction',
    # 'dictionary.sales_reestr',
    # 'ods__wings.r_par',
    # 'ods__wings.users_expired_consents',
    # 'ods__tranzaxis.avo_creditline_min_pay',
    # 'ods__fido.ln_card',
    # 'ods__odata_1c.accounting_register',
    # 'datamart.contract_classification_subaccount',
    # 'dictionary.avobank_account_mapping',
    # 'datamart.contract_classification_inactive_tech',
    'ods__appsflyer.uninstalls',
    'ods__wings.process_par_value',
    'ods__fido.v_region',
    'datamart.client_id_links',
    'ods__avo_registries.registries_records',
    'datamart.tx_turnover',
    'dictionary.operation_purpose',
    'datamart.contract_classification_delinquent_tech',
    'ods__ktc.clients',
    'ods__wings.rwa',
    'dictionary.mobile_notification_parts',
    'ods__fido_cod.accounts',
    'datamart.entry',
    'ods__asterisk.queue_log',
    'ods__fido.day_operational',
    'ods__crm.reasons1',
    'dictionary.mobile_tariff',
    'ods__wings.offers',
    'ods__avo_offers.command',
    'ods__tranzaxis_cod.avo_oper_day',
    'datamart.avo_users',
    'dictionary.contract_subaccounts',
    'datamart_cod.tx_turnover',
    'dictionary.operation_specification',
    'ods__wings.tasks_res',
    'ods__wings.tasks_req',
    'ods__tranzaxis.avo_account',
    'ods__tranzaxis.avo_entry_part',
    'ods__fido_cod.accounts_history',
    'datamart.clients_red',
    'datamart_cod.entry',
    'ods__tranzaxis.avo_terminal',
    'ods__fido.accounts',
    'datamart.avobank_ma_base',
    'ods__avo_operation_history.operations',
    'ods__avo_device_manager.user_devices',
    'dictionary.dmr_matrix_accounts',
    'ods__wings.card_history',
    'ods__fido.leads',
    'ods__avo_users.histories_users_actions',
    'ods__kafka.logger_events',
    'ods__tranzaxis.avo_doer',
    'ods__tranzaxis.avo_contract',
    'ods__wings.features',
    'ods__tranzaxis.avo_creditline_cycle',
    'dictionary.currencies',
    'ods__asterisk.cdr',
    'ods__fido.leads_history',
    'ods__avo_offers.offer',
    'dictionary.bank_account_register',
    'ods__avo_attempts.attempt',
    'datamart.contract_actual_tech',
    'datamart.katm_incomes',
    'ods__wings.process',
]

table_names = [
#'ods__kafka.logger_events',#69771833384, 1964742608
'datamart_cod.tx_turnover',#13486089528, 1608399029
#'datamart_cod.tx_turnover_per_day',#3816810608, 1608399029
'datamart.tx_turnover',#11766567032, 1599550711
#'datamart.tx_turnover_per_day',#2429563136, 1599524931
'datamart_cod.fido_turnover',#20190259920, 1122767927
'datamart.fido_turnover',#13124372776, 1104484413
'ods__tranzaxis.avo_doer',#12441272248, 1034983917
'ods__tranzaxis.avo_tran',#35846184272, 894310760
'datamart.avobank_ma_base',#6718285968, 813265375
'ods__wings.process_par_value',#6414874752, 421868155
'datamart_cod.entry',#1708552040, 347016192
'ods__tranzaxis.avo_entry_part',#2783259344, 345296049
'datamart.entry',#2293710832, 345119203
# 'temp.impressions',#0, 300380586
'ods__fido.transacts_history',#2283157888, 296350540
'datamart.contract_classification_subaccount',#1394442968, 234949870
'datamart.contract_actual_tech',#854833136, 234949870
'datamart.contract_classification',#582099528, 234949870
'ods__tranzaxis_cod.avo_entry',#464394288, 216876033
'ods__tranzaxis.avo_entry',#6414672552, 215667644
'ods__fido.leads_history',#5049468392, 149073392
'ods__tranzaxis_cod.avo_entry_part',#626365448, 132114292
'ods__fido.saldo',#1175325720, 92612905
'datamart.contract_classification_inactive_tech',#194284536, 86833047
'ods__appsflyer.inapps',#1502854032, 59530523
'ods__asterisk.cdr',#724156080, 54008232
'datamart.dmr_matrix',#327545408, 52452296
'datamart.avobank_ma_monthly',#5473304576, 48337661
'ods__avo_message_storage.messages',#1816720064, 39849267
'datamart.portfolio_daily',#1305013080, 39216206
'datamart.portfolio_daily_tranzaxis',#424139272, 38930105
'ods__appsflyer.sessions',#764577848, 37864194

# 'ods__appsflyer.clicks',#0, 31463528
# 'ods__avo_users.histories_users_actions',#483104520, 29558329
# 'ods__avo_automats.automat_status_history',#206479760, 25931896
# 'datamart.fido_card_turnover',#455302928, 18830618
# #'ods__asterisk.call_history',#211183704, 18207223
# 'ods__avo_operation_manager.transactions',#6109810736, 15793011
# 'ods__avo_attempts.attempt_history',#210681984, 14727360
# 'ods__asterisk.queue_log',#69183920, 13494195
# 'datamart.contract_classification_delinquent_tech',#21180032, 13112202
# 'datamart_cod.accounts',#77292976, 12547289
# 'ods__tranzaxis_cod.avo_account',#72020936, 12546374
# 'datamart.accounts',#558268416, 12510632
# 'ods__tranzaxis.avo_account',#922499528, 12510632
# 'ods__avo_operation_history.operations',#1543893848, 11948909
# 'ods__kafka.tx_events',#440969200, 11633239
# 'ods__wings.process',#593043040, 10457429
# 'datamart.portfolio',#57600880, 9654718
# 'ods__tranzaxis.avo_creditline_min_pay_delta',#48276568, 9308791
# 'ods__fido_cod.accounts_history',#16685864, 9295038
# 'ods__fido.accounts_history',#66461816, 9268536
# 'ods__fido_cod.accounts',#94668208, 9262849
# 'ods__fido.accounts',#90252320, 9262685
# 'ods__fido.accounts_y',#84333120, 9104735
# 'datamart.contract_classification_transactor_tech',#13758016, 8270995
# 'ods__tranzaxis.avo_creditline_cycle',#34039888, 8165634
# 'ods__avo_operation_manager.operations',#1323707920, 8010863
# 'ods__crm.template_answers',#16631488, 7703654
# 'ods__asterisk.auto_dial_outbound_campaign_data_set',#29657904, 7044075
# 'datamart.katm_incomes',#233441464, 6137395
# 'ods__tranzaxis.avo_contract',#46154912, 6083199
# 'datamart.contracts',#48008240, 6083199
# 'history__tranzaxis.avo_contract',#88190736, 5793491
# 'ods__avo_registries.registries_records',#2072394848, 5614810
# 'ods__wings.tasks_res',#42791240, 4837749
# 'datamart.logger_user_device_map',#17125200, 4717502
# 'datamart.soft_collection_queue_log',#61894432, 3670842
# 'ods__avo_attempts.attempt',#1568472200, 3340545
# 'datamart.wings_cl_events',#32067984, 2691057
# 'datamart.avo_users',#40736776, 2574321
# 'datamart.avo_app_install',#22983240, 2560802
# 'ods__appsflyer.installs',#103463184, 2479469
# 'ods__fido.transacts',#29044576, 2351950
# 'ods__avo_offers.command',#24732656, 2106151
# 'ods__crm.service_requests',#28280456, 1995003
# 'ods__wings.reports',#1723080192, 1992732
# 'ods__avo_device_manager.user_devices',#27133456, 1982448
# 'ods__wings.reports_request',#8943392, 1897433
# 'ods__wings.tasks_req',#96797088, 1810539
# 'ods__avo_users.app_users',#1996357632, 1667246
# 'datamart.avo_users_mapp',#393136752, 1661639
# 'datamart.katm_no_nesting_data',#209276024, 1635533
# 'datamart.device_kyc_success_events',#17201272, 1562317
# 'ods__tranzaxis.avo_card',#22443248, 1559733
# 'ods__tranzaxis.avo_token',#7607776, 1559732
# 'ods__wings.cases',#13410448, 1474239
# 'datamart_risks.application',#17764568, 1471169
# 'datamart.avo_users_kyc_oper',#24423528, 1411390
# 'ods__wings.card_history',#1456686496, 1395493
# 'ods__wings.features',#62761288, 1367573
# 'datamart.avo_users_kyc_daily',#76506808, 1278529
# 'ods__tranzaxis.avo_creditline_min_pay',#1751032, 1234373
# 'ods__fido.leads',#114982536, 1175975
# 'ods__fido.client_current',#12603944, 1148232
# 'datamart.creditline_contracts',#8149056, 1144757
# 'ods__tranzaxis.avo_creditline_contract',#739968, 1144757
# 'datamart_risks.myid',#36354072, 1144350
# 'datamart.client_id_links',#38240256, 1143800
# 'datamart.clients_red',#61014016, 1143800
# 'datamart.clients',#91783168, 1143800
# 'ods__appsflyer.uninstalls',#13356184, 931094
# 'datamart.avo_clients_offer_oper',#66582384, 867189
# 'ods__fido.saldo_y',#26918872, 715952
# 'ods__ktc.historical_cash_unit_status_p',#8278488, 477970
# 'ods__wings.users_expired_consents',#1656320, 464451
# 'ods__fido.dwh_coa',#8261064, 360716
# 'ods__avo_internal_cards.card',#7886168, 295453
# 'ods__wings.offers',#4250728, 283567
# 'ods__avo_offers.offer',#3128856, 283412
# 'datamart.avo_clients_offer_daily',#9421816, 184435
# 'ods__avo_offers.cl_transaction',#3083232, 178600
# 'datamart.automat_status',#1229272, 171971
# 'ods__fido.ln_card',#1458920, 164392
# 'ods__wings.rwa',#2082304, 145728
# 'ods__appsflyer.organic_uninstalls',#2608464, 138872
# 'ods__facebook.insights',#411560, 98268
# 'ods__facebook.ads',#429792, 95088
# 'anabasys_sandbox.humo_scorr_aug_to_sep',#26869760, 76999
# 'dictionary.sales_reestr',#770776, 72881
# 'ods__odata_1c.accounting_register',#2280040, 71999
# 'anabasys_sandbox.humo_scorr_aug',#24641536, 70204
# 'ods__minio.pd_model_monitoring',#171638064, 67165
# 'datamart.offer_clip',#996896, 65088
# 'ods__wings.temp_users_checked',#198536, 56432
# 'ods__jira.tickets_custex',#294632, 54336
# 'ods__uzcard.uzcard_tran_to_sep',#7989392, 53472
# 'ods__vk.statistics',#224992, 50056
# 'ods__uzcard.uzcard_tran_aug',#6995928, 46031
# 'ods__appsflyer.clicks_retargeting',#0, 45649
# 'ods__google_ads.ad_basic_stats',#163040, 45275
# 'ods__appsflyer.inapps_retargeting',#4049840, 30654
# 'ods__google_ads.ad_group',#17600, 29908
# 'ods__appsflyer.sessions_retargeting',#3919800, 23577
# 'ods__odata_1c.expenses',#1072488, 9897
# 'ods__google_ads.campaign',#10024, 9489
# 'ods__facebook.campaigns',#83656, 9486
# 'ods__avo_registries.registries',#264816, 4575
# 'ods__avo_marketplace.services',#205992, 4005
# 'ods__avo.currency_rates',#87504, 2944
# 'datamart.hr_employee_history',#41216, 2747
# 'datamart.tx_fido_oper_day',#32768, 1942
# 'datamart_cod.tx_fido_oper_day',#8904, 1942
# 'ods__fido.day_operational',#13208, 1942
# 'ods__fido_cod.day_operational',#13232, 1942
# 'ods__fido.v_coa',#57032, 1663
# 'ods__asterisk.auto_dial_outbound_campaign',#19352, 1644
# 'ods__wings.r_par',#549768, 1638
# 'ods__avo_marketplace.merchants',#163632, 1626
# 'ods__fido.dwh_coa_y',#46544, 1539
# 'dictionary.balance_code_ru',#37528, 1247
# 'dictionary.io_fraud',#5640, 1225
# 'ods__odata_1c.employee_personal_data',#43312, 1119
# 'ods__odata_1c.employee_register',#26760, 1115
# 'ods__odata_1c.employees',#34336, 1113
# 'dictionary.calendar',#23048, 1096
# 'ods__fido.dep_accounts',#15784, 978
# 'ods__odata_1c.accounts',#44448, 896
# 'ods__tranzaxis_cod.avo_oper_day',#1736, 602
# 'ods__tranzaxis.avo_oper_day',#16512, 601
# 'ods__asterisk.agent_queues',#1160, 529
# 'ods__odata_1c.counterparties',#92120, 491
# 'ods__asterisk.agents',#2440, 398
# 'ods__fido.dep_contracts',#16088, 315
# 'dictionary.avobank_account_mapping',#20840, 298
# 'datamart.hr_dep_structure',#32768, 251
# 'dictionary.bank_account_register',#7544, 251
# 'dictionary.countries',#7712, 238
# 'ods__vk.banners',#16976, 235
# 'ods__tranzaxis.avo_account_plan_item',#16576, 195
# 'ods__odata_1c.expenses_dict',#7128, 185
# 'ods__avo_automats.automat',#8048, 178
# 'ods__crm.template_questions',#1792, 173
# 'dictionary.currencies',#4720, 155
# 'ods__odata_1c.departments',#4520, 142
# 'ods__wings.guides',#6504, 133
# 'dictionary.mobile_standart_notification',#4344, 119
# 'dictionary.operation_specification',#2392, 87
# 'ods__tranzaxis.avo_terminal',#2440, 67
# 'dictionary.countries_offshore',#1568, 67
# 'dictionary.mobile_tariff',#1144, 66
# 'ods__ktc.clients',#2632, 65
# 'dictionary.media_group',#840, 63
# 'dictionary.atm_point_info',#4152, 59
# 'dictionary.dmr_matrix_accounts',#456, 51
# 'dictionary.attraction_channel',#800, 50
# 'dictionary.operation_scheme',#1048, 39
# 'ods__tranzaxis.avo_contract_type',#1224, 29
# 'ods__avo_marketplace.categories',#0, 27
# 'dictionary.contract_subaccounts',#784, 26
# 'dictionary.fatf_high_risk_list',#944, 25
# 'dictionary.offer_status_mapp',#872, 24
# 'dictionary.mobile_notification_parts',#704, 20
# 'dictionary.operation_purpose',#480, 19
# 'dictionary.regions_ru',#728, 14
# 'ods__fido.v_region',#848, 14
# 'dictionary.mobile_code',#464, 14
# 'ods__ktc.cash_unit_statuses',#360, 12
# 'dictionary.ltv_marketing_expences_mapping',#312, 10
# 'ods__crm.reasons1',#280, 9
# 'dictionary.bonus_offers',#928, 8
# 'dictionary.offer_status',#592, 8
# 'ods__odata_1c.cfo',#688, 8
# 'dictionary.mobile_operator',#336, 7
# 'ods__tranzaxis.avo_token_life_phase',#1248, 7
# 'dictionary.contract_class',#536, 7
# 'ods__tranzaxis.avo_token_product',#480, 6
# 'ods__tranzaxis.avo_card_product',#520, 6
# 'ods__odata_1c.currencies',#496, 4
# 'dictionary.mobile_notification_type',#384, 4
# 'dictionary.automat_ping_interval',#520, 3
# 'ods__vk.groups',#792, 1
]

def fetch_table_metadata( table_schema, table_name):
    query = f"""
        SELECT table_name, column_name, data_type, udt_name, ordinal_position
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
          AND table_schema = '{table_schema}'
        ORDER BY ordinal_position;
    """
    try:
        conn = conn_db()
        cursor = conn.cursor()
        cursor.execute(query)
        columns = cursor.fetchall()
        cursor.close()
        conn.close()
        return columns
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return []

def generate_pxf_ddl(table_schema, table_name, columns):
    ddl = f"CREATE EXTERNAL TABLE {table_schema}.{table_name}_ext (\n"
    for column in columns:
        column_name = column[1]
        data_type = column[2]
        ddl += f"    {column_name} {data_type},\n"
    ddl = ddl.rstrip(",\n") + "\n)\n"
    ddl += f"""LOCATION ('pxf://{table_schema}.{table_name}?PROFILE=JDBC&SERVER=gp_dwh')
     ON ALL
        FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
        ENCODING 'UTF8';"""
    return ddl

greenplum_to_pxf_types = {
    "int2": "smallint",
    "int4": "integer",
    "int8": "bigint",
    "float4": "real",
    "float8": "double precision",
    "numeric": "numeric",
    "bool": "boolean",
    "text": "text",
    "varchar": "varchar",
    "bpchar": "char",
    "date": "date",
    "timestamp": "timestamp",
    "timestamptz": "text",
    "uuid": "text",
    "json": "text",
    "jsonb": "text",
    "bytea": "bytea",
    "interval": "text",
    "unknown": "text"
}

def generate_pxf_ddl(table_schema, table_name, columns):
    ddl = f"drop EXTERNAL TABLE if exists {table_schema}.{table_name}_ext; \n"
    ddl += f"CREATE EXTERNAL TABLE {table_schema}.{table_name}_ext (\n"
    for column in columns:
        column_name = column[1]
        if column_name.upper() in pgsql_keywords:
            column_name = f'"{column_name}"'
        udt_name = column[3].lower()
        data_type = greenplum_to_pxf_types.get(udt_name, "text")
        ddl += f"    {column_name} {data_type},\n"
    ddl = ddl.rstrip(",\n") + "\n)"
    ddl += f"""
LOCATION (
    'pxf://{table_schema}.{table_name}?PROFILE=JDBC&SERVER=gp_dwh'    
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import')
ENCODING 'UTF8'"""
    return ddl

def handle_unsupported_fields(columns):
    unsupported_fields = []

    for column in columns:
        column_name = column[1]
        udt_name = column[3].lower()
        if udt_name not in greenplum_to_pxf_types:
            print (f'unsupported_field {udt_name}')
            unsupported_fields.append((column_name, "text"))  # Default to "text" for unsupported types

    if unsupported_fields:
        print("\nUnsupported fields detected:")
        for field in unsupported_fields:
            print(f"- Column: {field[0]}, Converted Data Type: {field[1]}")

    return unsupported_fields

def generate_insert_with_cast(table_schema, table_name, columns):
    insert_stmt = f"INSERT INTO {table_schema}.{table_name} SELECT "
    cast_expressions = []

    for column in columns:
        column_name = column[1]
        if column_name.upper() in pgsql_keywords:
            column_name = f'"{column_name}"'
        udt_name = column[3].lower()
        pxf_type = greenplum_to_pxf_types.get(udt_name)
        if pxf_type:
            cast_expressions.append(f"CAST({column_name} AS {udt_name}) AS {column_name}")
        elif udt_name=="unknown":
            cast_expressions.append(f"CAST({column_name} AS text) AS {column_name}")
        else:
            cast_expressions.append(f"{column_name} {udt_name}")

    insert_stmt += ",\n".join(cast_expressions)
    insert_stmt += f" FROM {table_schema}.{table_name}_ext;"
    return insert_stmt

def make_script(ddl,table_schema, table_name):
    script = ''
    script+=("DO\n$$\n")
    script+=("""DECLARE
        vsqlerrm TEXT;
        vsqlcode TEXT;
        v_context TEXT;
        vcheck int default 0;
        vcheck1 int default 0;
    BEGIN
        BEGIN\n""")
    script+=("\n")
    # script+=(f"truncate table {table_schema}.{table_name};")
    script+=("\n\n")
    script+=(f"-- {table_schema}.{table_name}\n")
    script+=(f"{ddl};")
    script+=("\n\n")
    script+="""select count() into vcheck1 from public.get_free_memory_segment() where replace(percent_usage,'%','')::int>97;\n if vcheck1 > 0 then\n
           RAISE NOTICE ' there is little space left %', vcheck1;\n
           end if; \n
           """
    script+=f"""select count() into vcheck from {table_schema}.{table_name} limit 1;\n if vcheck = 0 and vcheck1=0 then\n"""
    script += f"""ANALYZE {table_schema}.{table_name};\n"""
    insert_stmt = generate_insert_with_cast(table_schema, table_name, columns_metadata)
    script+=(f"{insert_stmt}\n")
    script += f"""ANALYZE {table_schema}.{table_name};\n"""
    script += """end if;\n"""
    script+=(f"""\n    EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS vsqlerrm = MESSAGE_TEXT, vsqlcode = RETURNED_SQLSTATE, v_context = PG_EXCEPTION_CONTEXT;
            -- Raise a notice with the error details
            RAISE NOTICE 'Error occurred in {table_schema}.{table_name} operation. SQLCODE: %, SQLERRM: %, CONTEXT: %', vsqlcode, vsqlerrm, v_context;
        END;\nEND;\n$$\n""")
    script+=("\n\n")
    return script

def exec_script_with_timeout(query, timeout):
    """
    Execute a SQL script with a timeout.
    """
    def run_query(q,conn,cursor):
        res = ''
        try:
            cursor.execute(query)
            if conn.notices:
                 for notice in conn.notices:
                    res+=notice
            #conn.commit()
            q.put(res)
        except Exception as e:
            q.put(f"Error executing script: {str(e)}")

    q = queue.Queue()
    conn = conn_cb()
    conn.autocommit = True
    cursor = conn.cursor()
    thread = Thread(target=run_query, args=(q,conn,cursor))
    print(f'start {query[0]}...', end='')
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        cursor.close()
        conn.close()
        print('TimeoutError')
        raise TimeoutError("Execution of the script timed out.")
    else:
        cursor.close()
        conn.close()
        print('ok')
        return q.get()


def run_vacuum_full_if_low_space():
    try:
        conn = conn_cb()
        conn.autocommit = True
        cursor = conn.cursor()

        disk_space_query = "SELECT dfhostname, dfspace FROM gp_toolkit.gp_disk_free WHERE dfspace < 30;"

        cursor.execute(disk_space_query)
        low_space_hosts = cursor.fetchall()

        if not low_space_hosts:
            return

        print("Low disk space detected on the following hosts:")
        for host, space in low_space_hosts:
            print(f"Host: {host}, Free Space: {space}%")

        print("Starting VACUUM FULL on the entire database...")
        cursor.execute("VACUUM FULL;")
        print("VACUUM FULL completed successfully.")

    except Exception as error:
        print(f"Error: {error}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    file_name = f"ext_tables_ddl.sql"
    pth = r'/Users/avotech/Library/DBeaverData/workspace6/General/Scripts'
    file_name = os.path.join(pth, file_name)
    if os.path.exists(file_name):
        os.remove(file_name)
    output = []
    for t_n in table_names:
        print("                 -----------                 ")
        table_schema = t_n.split('.')[0]
        table_name = t_n.split('.')[1]
        columns_metadata = fetch_table_metadata( table_schema, table_name)

        if columns_metadata:
            unsupported_fields = handle_unsupported_fields(columns_metadata)
            if unsupported_fields:
                raise Exception(f"{table_schema}.{table_name} - {str(unsupported_fields)}");
            if not unsupported_fields:
                ddl = generate_pxf_ddl(table_schema, table_name, columns_metadata)
                print(f"{table_schema}.{table_name} - make script - ok")
                #print(ddl)
                with open(file_name, "a") as file:
                    script = make_script(ddl,table_schema, table_name)
                    try:
                        #res_script = exec_script_with_timeout(script, timeout=1200)
                        res_script=''
                        if res_script.find('Error occurred')<0:
                            file.write(f"--{table_schema}.{table_name} - ok")
                            print(f"{table_schema}.{table_name} - import - ok")
                            #print(exec_script_with_timeout(f'vacuum {table_schema}.{table_name}', timeout=120))
                        else:
                            file.write(f"--{table_schema}.{table_name} - err\n--{res_script}\n{script}")
                            print(f"{table_schema}.{table_name} - !!!!!!!! import - ERR\n")
                    #    run_vacuum_full_if_low_space()
                    except Exception as e:
                        file.write(f"--{script}\n --error\n{str(e)}\n")
                        print(f"Error execute import script: for {table_schema}.{table_name} - {e}")
                file_name1 = os.path.join(pth, '0', f'{table_schema}_{table_name}.sql')
                with open(file_name1, "w") as file:
                    file.write(script)
                    output.append(
                        f'psql -d dwh_cloudberrydb -f /tmp/{table_schema}_{table_name}.sql > /tmp/{table_schema}_{table_name}.log')
            else:
                print("\nSome fields are not supported in PXF. Please review the unsupported fields.")
        else:
            print(f"{table_schema}, {table_name} - No metadata found or an error occurred.")
    print ('\n'.join(output))