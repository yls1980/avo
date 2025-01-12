import psycopg2
from psycopg2 import sql
import secr

# Database connection parameters
DB_CONFIG = {
    'host': "10.122.3.134",
    'port': "5432",
    'database': "greenplum-dwh",
    'user': "gpadmin",
    'password': secr.pss()
}

BATCH_SIZE = 25000

def batch_load():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        offset = 0
        while True:
            cursor.execute(sql.SQL("""
                SELECT tmp.*
                FROM ods__appsflyer.impressions_tmp tmp
                LEFT JOIN ods__appsflyer.impressions imp
                ON md5(coalesce(tmp."key", ''))::uuid = md5(coalesce(imp."key", ''))::uuid
                WHERE imp."key" IS NULL
                LIMIT {batch_size} OFFSET {batch_offset};
            """).format(batch_size=sql.Literal(BATCH_SIZE), batch_offset=sql.Literal(offset)))

            batch = cursor.fetchall()
            if not batch:
                break

            cursor.executemany("""
                INSERT INTO temp.temp_ins_table (
                    attributed_touch_type, attributed_touch_time, install_time, event_time, event_name,
                    event_value, event_revenue, event_revenue_currency, event_revenue_usd, af_cost_model,
                    af_cost_value, af_cost_currency, event_source, is_receipt_validated, af_prt,
                    media_source, af_channel, af_keywords, install_app_store, campaign, af_c_id,
                    af_adset, af_adset_id, af_ad, af_ad_id, af_ad_type, af_siteid, af_sub_siteid, 
                    af_sub1, af_sub2, af_sub3, af_sub4, af_sub5, contributor_1_touch_type, 
                    contributor_1_touch_time, contributor_1_af_prt, contributor_1_match_type, 
                    contributor_1_media_source, contributor_1_campaign, contributor_2_touch_type, 
                    contributor_2_touch_time, contributor_2_af_prt, contributor_2_media_source, 
                    contributor_2_campaign, contributor_2_match_type, contributor_3_touch_type, 
                    contributor_3_touch_time, contributor_3_af_prt, contributor_3_media_source, 
                    contributor_3_campaign, contributor_3_match_type, region, country_code, state, 
                    city, postal_code, dma, ip, wifi, "operator", carrier, "language", appsflyer_id, 
                    customer_user_id, android_id, advertising_id, imei, idfa, idfv, amazon_aid, 
                    device_type, device_category, platform, os_version, app_version, sdk_version, 
                    app_id, app_name, bundle_id, is_retargeting, retargeting_conversion_type, 
                    is_primary_attribution, af_attribution_lookback, af_reengagement_window, 
                    match_type, user_agent, http_referrer, original_url, gp_referrer, gp_click_time, 
                    gp_install_begin, gp_broadcast_referrer, custom_data, network_account_id, 
                    keyword_match_type, blocked_reason, blocked_reason_value, blocked_reason_rule, 
                    blocked_sub_reason, af_web_id, web_event_type, media_type, pid, utm_source, 
                    utm_medium, utm_term, utm_content, utm_campaign, device_download_time, 
                    deeplink_url, oaid, media_channel, event_url, utm_id, ad_unit, "segment", 
                    placement, mediation_network, impressions, monetization_network, conversion_type, 
                    campaign_type, device_model, att, custom_dimension, is_lat, app_type, keyword_id, 
                    validation_reason_value, rejected_reason, fraud_reason, fraud_sub_reason, 
                    is_organic, detection_date, store_product_page, device_id_type, ad_placement, 
                    app_group_id, app_store_ids, "key", last_modified_dt, dwh_job_id, dwh_created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
            """, batch)

            cursor.executemany("""
                INSERT INTO ods__appsflyer.impressions (
                    attributed_touch_type, attributed_touch_time, install_time, event_time, event_name,
                    event_value, event_revenue, event_revenue_currency, event_revenue_usd, af_cost_model,
                    af_cost_value, af_cost_currency, event_source, is_receipt_validated, af_prt,
                    media_source, af_channel, af_keywords, install_app_store, campaign, af_c_id,
                    af_adset, af_adset_id, af_ad, af_ad_id, af_ad_type, af_siteid, af_sub_siteid, 
                    af_sub1, af_sub2, af_sub3, af_sub4, af_sub5, contributor_1_touch_type, 
                    contributor_1_touch_time, contributor_1_af_prt, contributor_1_match_type, 
                    contributor_1_media_source, contributor_1_campaign, contributor_2_touch_type, 
                    contributor_2_touch_time, contributor_2_af_prt, contributor_2_media_source, 
                    contributor_2_campaign, contributor_2_match_type, contributor_3_touch_type, 
                    contributor_3_touch_time, contributor_3_af_prt, contributor_3_media_source, 
                    contributor_3_campaign, contributor_3_match_type, region, country_code, state, 
                    city, postal_code, dma, ip, wifi, "operator", carrier, "language", appsflyer_id, 
                    customer_user_id, android_id, advertising_id, imei, idfa, idfv, amazon_aid, 
                    device_type, device_category, platform, os_version, app_version, sdk_version, 
                    app_id, app_name, bundle_id, is_retargeting, retargeting_conversion_type, 
                    is_primary_attribution, af_attribution_lookback, af_reengagement_window, 
                    match_type, user_agent, http_referrer, original_url, gp_referrer, gp_click_time, 
                    gp_install_begin, gp_broadcast_referrer, custom_data, network_account_id, 
                    keyword_match_type, blocked_reason, blocked_reason_value, blocked_reason_rule, 
                    blocked_sub_reason, af_web_id, web_event_type, media_type, pid, utm_source, 
                    utm_medium, utm_term, utm_content, utm_campaign, device_download_time, 
                    deeplink_url, oaid, media_channel, event_url, utm_id, ad_unit, "segment", 
                    placement, mediation_network, impressions, monetization_network, conversion_type, 
                    campaign_type, device_model, att, custom_dimension, is_lat, app_type, keyword_id, 
                    validation_reason_value, rejected_reason, fraud_reason, fraud_sub_reason, 
                    is_organic, detection_date, store_product_page, device_id_type, ad_placement, 
                    app_group_id, app_store_ids, "key", last_modified_dt, dwh_job_id, dwh_created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
            """, batch)

            print(f"Processed batch with offset {offset}, batch size: {len(batch)}")

            offset += BATCH_SIZE

        cursor.execute("""
            SELECT COUNT(*)
            FROM ods__appsflyer.impressions imp
            WHERE imp."key" IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM ods__appsflyer.impressions_keys keys
                WHERE imp."key" = keys."key"
            );
        """)
        missing_keys_count = cursor.fetchone()[0]
        print(f"Missing keys: {missing_keys_count}")

    except psycopg2.Error as e:
        print(f"Database error: {e}")

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    batch_load()
