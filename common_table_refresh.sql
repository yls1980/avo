
DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.attempt_error
 ALTER TABLE dictionary.attempt_error RENAME TO attempt_error_old;
 




CREATE TABLE dictionary.attempt_error (
    id integer,
    code text,
    name text,
    description text,
    is_refuse integer,
    valid_from timestamp without time zone,
    valid_to timestamp without time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.attempt_error OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.attempt_error TO gpadmin;




 insert into dictionary.attempt_error
 select * from attempt_error_old;
 analyze dictionary.attempt_error;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.attraction_channel
 ALTER TABLE dictionary.attraction_channel RENAME TO attraction_channel_old;
 




CREATE TABLE dictionary.attraction_channel (
    media_source text,
    agg_media_source text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.attraction_channel OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.attraction_channel TO gpadmin;




 insert into dictionary.attraction_channel
 select * from attraction_channel_old;
 analyze dictionary.attraction_channel;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.automat_ping_interval
 ALTER TABLE dictionary.automat_ping_interval RENAME TO automat_ping_interval_old;
 




CREATE TABLE dictionary.automat_ping_interval (
    id integer,
    type text,
    description text,
    lag integer,
    date_from date,
    date_to date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.automat_ping_interval OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.automat_ping_interval TO gpadmin;




 insert into dictionary.automat_ping_interval
 select * from automat_ping_interval_old;
 analyze dictionary.automat_ping_interval;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.avobank_account_mapping
 ALTER TABLE dictionary.avobank_account_mapping RENAME TO avobank_account_mapping_old;
 




CREATE TABLE dictionary.avobank_account_mapping (
    balance_group integer,
    account_number text,
    name text,
    grouping_note text,
    grouping_fs text,
    cash_none_cash text,
    cash_grouping text,
    cash_classification text,
    ma_level_1 text,
    ma_level_2 text,
    ma_level_3 text,
    capex_opex text,
    bspl text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.avobank_account_mapping OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.avobank_account_mapping TO gpadmin;




 insert into dictionary.avobank_account_mapping
 select * from avobank_account_mapping_old;
 analyze dictionary.avobank_account_mapping;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.balance_code_ru
 ALTER TABLE dictionary.balance_code_ru RENAME TO balance_code_ru_old;
 




CREATE TABLE dictionary.balance_code_ru (
    id integer,
    code text,
    name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.balance_code_ru OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.balance_code_ru TO gpadmin;




 insert into dictionary.balance_code_ru
 select * from balance_code_ru_old;
 analyze dictionary.balance_code_ru;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.bank_account_register
 ALTER TABLE dictionary.bank_account_register RENAME TO bank_account_register_old;
 




CREATE TABLE dictionary.bank_account_register (
    id text,
    cfo_2_lvl text,
    expense_description text,
    cost_center text,
    type text,
    t_i text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.bank_account_register OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.bank_account_register TO gpadmin;




 insert into dictionary.bank_account_register
 select * from bank_account_register_old;
 analyze dictionary.bank_account_register;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.bonus_offers
 ALTER TABLE dictionary.bonus_offers RENAME TO bonus_offers_old;
 




CREATE TABLE dictionary.bonus_offers (
    id integer,
    product_category_code text,
    product_type_code_tx text,
    product_code text,
    product_description text,
    channel text,
    parent_product_category_code text,
    value integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.bonus_offers OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.bonus_offers TO gpadmin;




 insert into dictionary.bonus_offers
 select * from bonus_offers_old;
 analyze dictionary.bonus_offers;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.calendar
 ALTER TABLE dictionary.calendar RENAME TO calendar_old;
 




CREATE TABLE dictionary.calendar (
    date date,
    day_of_year smallint,
    year smallint,
    month smallint,
    month_ru text,
    month_ru_year text,
    day smallint,
    start_of_month date,
    end_of_month date,
    week_day smallint,
    week_day_ru text,
    quarter integer,
    start_of_quarter date,
    end_of_quarter date,
    iso_week integer,
    absolute_week smallint,
    week_days text,
    is_non_working boolean,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.calendar OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.calendar TO gpadmin;




 insert into dictionary.calendar
 select * from calendar_old;
 analyze dictionary.calendar;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.code_mcc
 ALTER TABLE dictionary.code_mcc RENAME TO code_mcc_old;
 




CREATE TABLE dictionary.code_mcc (
    id integer,
    code_mcc integer,
    description text,
    is_privileged integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.code_mcc OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.code_mcc TO gpadmin;




 insert into dictionary.code_mcc
 select * from code_mcc_old;
 analyze dictionary.code_mcc;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.contract_class
 ALTER TABLE dictionary.contract_class RENAME TO contract_class_old;
 




CREATE TABLE dictionary.contract_class (
    id integer,
    name text,
    description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.contract_class OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.contract_class TO gpadmin;




 insert into dictionary.contract_class
 select * from contract_class_old;
 analyze dictionary.contract_class;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.contract_subaccounts
 ALTER TABLE dictionary.contract_subaccounts RENAME TO contract_subaccounts_old;
 




CREATE TABLE dictionary.contract_subaccounts (
    id integer,
    sub_code text,
    class_id integer,
    description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.contract_subaccounts OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.contract_subaccounts TO gpadmin;




 insert into dictionary.contract_subaccounts
 select * from contract_subaccounts_old;
 analyze dictionary.contract_subaccounts;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.countries
 ALTER TABLE dictionary.countries RENAME TO countries_old;
 




CREATE TABLE dictionary.countries (
    id smallint,
    code_a2 text,
    code_a3 text,
    code_number text,
    name_rus text,
    name_eng text,
    currency text,
    domain text,
    phone_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.countries OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.countries TO gpadmin;




 insert into dictionary.countries
 select * from countries_old;
 analyze dictionary.countries;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.countries_offshore
 ALTER TABLE dictionary.countries_offshore RENAME TO countries_offshore_old;
 




CREATE TABLE dictionary.countries_offshore (
    id text,
    name_eng text,
    list_type text,
    code_a2 text,
    code_a3 text,
    date_from text,
    date_to text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.countries_offshore OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.countries_offshore TO gpadmin;




 insert into dictionary.countries_offshore
 select * from countries_offshore_old;
 analyze dictionary.countries_offshore;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.currencies
 ALTER TABLE dictionary.currencies RENAME TO currencies_old;
 




CREATE TABLE dictionary.currencies (
    id smallint,
    code_iso text,
    code_number text,
    name_rus text,
    name_eng text,
    change_currency_rus text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.currencies OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.currencies TO gpadmin;




 insert into dictionary.currencies
 select * from currencies_old;
 analyze dictionary.currencies;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.dmr_matrix_accounts
 ALTER TABLE dictionary.dmr_matrix_accounts RENAME TO dmr_matrix_accounts_old;
 




CREATE TABLE dictionary.dmr_matrix_accounts (
    account_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.dmr_matrix_accounts OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.dmr_matrix_accounts TO gpadmin;




 insert into dictionary.dmr_matrix_accounts
 select * from dmr_matrix_accounts_old;
 analyze dictionary.dmr_matrix_accounts;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.fatf_high_risk_list
 ALTER TABLE dictionary.fatf_high_risk_list RENAME TO fatf_high_risk_list_old;
 




CREATE TABLE dictionary.fatf_high_risk_list (
    id integer,
    name_eng text,
    list_type text,
    code_a2 text,
    code_a3 text,
    date_from date,
    date_to date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.fatf_high_risk_list OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.fatf_high_risk_list TO gpadmin;




 insert into dictionary.fatf_high_risk_list
 select * from fatf_high_risk_list_old;
 analyze dictionary.fatf_high_risk_list;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.hr_payments_classifier
 ALTER TABLE dictionary.hr_payments_classifier RENAME TO hr_payments_classifier_old;
 




CREATE TABLE dictionary.hr_payments_classifier (
    syncode text,
    code_1c text,
    name_1c text,
    code_iabs text,
    name_iabs text,
    average_salary_calc text,
    annual_bonus_calc text,
    income_code_ndfl text,
    income_type_insurance_contributions text,
    payment_category_detailed_analytics text,
    payment_category_total_analytics text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.hr_payments_classifier OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.hr_payments_classifier TO gpadmin;




 insert into dictionary.hr_payments_classifier
 select * from hr_payments_classifier_old;
 analyze dictionary.hr_payments_classifier;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.humo_card_tran_type
 ALTER TABLE dictionary.humo_card_tran_type RENAME TO humo_card_tran_type_old;
 




CREATE TABLE dictionary.humo_card_tran_type (
    tran_type_id text,
    description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.humo_card_tran_type OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.humo_card_tran_type TO gpadmin;




 insert into dictionary.humo_card_tran_type
 select * from humo_card_tran_type_old;
 analyze dictionary.humo_card_tran_type;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.kyc_status
 ALTER TABLE dictionary.kyc_status RENAME TO kyc_status_old;
 




CREATE TABLE dictionary.kyc_status (
    id integer,
    name text,
    description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.kyc_status OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.kyc_status TO gpadmin;




 insert into dictionary.kyc_status
 select * from kyc_status_old;
 analyze dictionary.kyc_status;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.loan_account_type
 ALTER TABLE dictionary.loan_account_type RENAME TO loan_account_type_old;
 




CREATE TABLE dictionary.loan_account_type (
    id integer,
    loan_account_type_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.loan_account_type OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.loan_account_type TO gpadmin;




 insert into dictionary.loan_account_type
 select * from loan_account_type_old;
 analyze dictionary.loan_account_type;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.logger_event_mapp
 ALTER TABLE dictionary.logger_event_mapp RENAME TO logger_event_mapp_old;
 




CREATE TABLE dictionary.logger_event_mapp (
    id integer,
    screen text,
    action text,
    event_name text,
    source_id integer,
    event_description text,
    section text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.logger_event_mapp OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.logger_event_mapp TO gpadmin;




 insert into dictionary.logger_event_mapp
 select * from logger_event_mapp_old;
 analyze dictionary.logger_event_mapp;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.logger_screens
 ALTER TABLE dictionary.logger_screens RENAME TO logger_screens_old;
 




CREATE TABLE dictionary.logger_screens (
    id integer,
    screen_name text,
    screen_description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.logger_screens OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.logger_screens TO gpadmin;




 insert into dictionary.logger_screens
 select * from logger_screens_old;
 analyze dictionary.logger_screens;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mapping_acc_number
 ALTER TABLE dictionary.mapping_acc_number RENAME TO mapping_acc_number_old;
 




CREATE TABLE dictionary.mapping_acc_number (
    account_number integer,
    account_group integer,
    balance_group integer,
    group_level_1 integer,
    group_level_2 text,
    deposits_and_securities text,
    liquidity text,
    commitments text,
    capital text,
    income text,
    analysis_of_interest text,
    account_description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mapping_acc_number OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mapping_acc_number TO gpadmin;




 insert into dictionary.mapping_acc_number
 select * from mapping_acc_number_old;
 analyze dictionary.mapping_acc_number;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.media_group
 ALTER TABLE dictionary.media_group RENAME TO media_group_old;
 




CREATE TABLE dictionary.media_group (
    media_source text,
    media_group text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.media_group OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.media_group TO gpadmin;




 insert into dictionary.media_group
 select * from media_group_old;
 analyze dictionary.media_group;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mobile_code
 ALTER TABLE dictionary.mobile_code RENAME TO mobile_code_old;
 




CREATE TABLE dictionary.mobile_code (
    id integer,
    mobile_operator_id integer,
    mobile_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mobile_code OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mobile_code TO gpadmin;




 insert into dictionary.mobile_code
 select * from mobile_code_old;
 analyze dictionary.mobile_code;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mobile_notification_channel
 ALTER TABLE dictionary.mobile_notification_channel RENAME TO mobile_notification_channel_old;
 




CREATE TABLE dictionary.mobile_notification_channel (
    id integer,
    channel_desc text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mobile_notification_channel OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mobile_notification_channel TO gpadmin;




 insert into dictionary.mobile_notification_channel
 select * from mobile_notification_channel_old;
 analyze dictionary.mobile_notification_channel;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mobile_notification_group
 ALTER TABLE dictionary.mobile_notification_group RENAME TO mobile_notification_group_old;
 




CREATE TABLE dictionary.mobile_notification_group (
    id integer,
    group_code text,
    group_desc text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mobile_notification_group OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mobile_notification_group TO gpadmin;




 insert into dictionary.mobile_notification_group
 select * from mobile_notification_group_old;
 analyze dictionary.mobile_notification_group;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mobile_notification_parts
 ALTER TABLE dictionary.mobile_notification_parts RENAME TO mobile_notification_parts_old;
 




CREATE TABLE dictionary.mobile_notification_parts (
    id integer,
    part_amount integer,
    coding text,
    notification_language text,
    lower_limit integer,
    upper_limit integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mobile_notification_parts OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mobile_notification_parts TO gpadmin;




 insert into dictionary.mobile_notification_parts
 select * from mobile_notification_parts_old;
 analyze dictionary.mobile_notification_parts;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mobile_notification_type
 ALTER TABLE dictionary.mobile_notification_type RENAME TO mobile_notification_type_old;
 




CREATE TABLE dictionary.mobile_notification_type (
    id integer,
    type_desc text,
    is_standart smallint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mobile_notification_type OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mobile_notification_type TO gpadmin;




 insert into dictionary.mobile_notification_type
 select * from mobile_notification_type_old;
 analyze dictionary.mobile_notification_type;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mobile_operator
 ALTER TABLE dictionary.mobile_operator RENAME TO mobile_operator_old;
 




CREATE TABLE dictionary.mobile_operator (
    id integer,
    operator_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mobile_operator OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mobile_operator TO gpadmin;




 insert into dictionary.mobile_operator
 select * from mobile_operator_old;
 analyze dictionary.mobile_operator;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mobile_standart_notification
 ALTER TABLE dictionary.mobile_standart_notification RENAME TO mobile_standart_notification_old;
 




CREATE TABLE dictionary.mobile_standart_notification (
    id integer,
    notification_code text,
    notification_desc text,
    "group_id" integer,
    channel_id integer,
    type_id integer,
    valid_from date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mobile_standart_notification OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mobile_standart_notification TO gpadmin;




 insert into dictionary.mobile_standart_notification
 select * from mobile_standart_notification_old;
 analyze dictionary.mobile_standart_notification;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.mobile_tariff
 ALTER TABLE dictionary.mobile_tariff RENAME TO mobile_tariff_old;
 




CREATE TABLE dictionary.mobile_tariff (
    id integer,
    mobile_operator_id integer,
    type_id integer,
    tariff integer,
    currency text,
    date_from date,
    date_to date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.mobile_tariff OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.mobile_tariff TO gpadmin;




 insert into dictionary.mobile_tariff
 select * from mobile_tariff_old;
 analyze dictionary.mobile_tariff;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.offer_status
 ALTER TABLE dictionary.offer_status RENAME TO offer_status_old;
 




CREATE TABLE dictionary.offer_status (
    id integer,
    name text,
    description text,
    priority integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.offer_status OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.offer_status TO gpadmin;




 insert into dictionary.offer_status
 select * from offer_status_old;
 analyze dictionary.offer_status;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.offer_status_mapp
 ALTER TABLE dictionary.offer_status_mapp RENAME TO offer_status_mapp_old;
 




CREATE TABLE dictionary.offer_status_mapp (
    id integer,
    command_status text,
    offer_status text,
    cl_application_status text,
    status_id integer,
    from_date date,
    to_date date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.offer_status_mapp OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.offer_status_mapp TO gpadmin;




 insert into dictionary.offer_status_mapp
 select * from offer_status_mapp_old;
 analyze dictionary.offer_status_mapp;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.operation_purpose
 ALTER TABLE dictionary.operation_purpose RENAME TO operation_purpose_old;
 




CREATE TABLE dictionary.operation_purpose (
    id integer,
    purpose_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.operation_purpose OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.operation_purpose TO gpadmin;




 insert into dictionary.operation_purpose
 select * from operation_purpose_old;
 analyze dictionary.operation_purpose;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.operation_scheme
 ALTER TABLE dictionary.operation_scheme RENAME TO operation_scheme_old;
 




CREATE TABLE dictionary.operation_scheme (
    id integer,
    scheme_code text,
    scheme_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.operation_scheme OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.operation_scheme TO gpadmin;




 insert into dictionary.operation_scheme
 select * from operation_scheme_old;
 analyze dictionary.operation_scheme;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.operation_specification
 ALTER TABLE dictionary.operation_specification RENAME TO operation_specification_old;
 




CREATE TABLE dictionary.operation_specification (
    id integer,
    specification_code text,
    scheme_id integer,
    purpose_id integer,
    specification_description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.operation_specification OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.operation_specification TO gpadmin;




 insert into dictionary.operation_specification
 select * from operation_specification_old;
 analyze dictionary.operation_specification;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.regions_ru
 ALTER TABLE dictionary.regions_ru RENAME TO regions_ru_old;
 




CREATE TABLE dictionary.regions_ru (
    code integer,
    name_uz text,
    name_ru text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.regions_ru OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.regions_ru TO gpadmin;




 insert into dictionary.regions_ru
 select * from regions_ru_old;
 analyze dictionary.regions_ru;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.sales_points
 ALTER TABLE dictionary.sales_points RENAME TO sales_points_old;
 




CREATE TABLE dictionary.sales_points (
    macroregion text,
    region text,
    area text,
    supervisor text,
    type text,
    sector text,
    code text,
    name text,
    status text,
    address text,
    landmark text,
    yandex_maps_link text,
    latitude text,
    longitude text,
    partner text,
    square numeric,
    card_vending_machine smallint,
    atm smallint,
    working_hours text,
    operating_mode text,
    opening_date date,
    closing_date date,
    is_allday text,
    available_services_list text,
    beeline_download numeric,
    beeline_load numeric,
    beeline_ping text,
    ucell_download numeric,
    ucell_load numeric,
    ucell_ping text,
    uzmobile_download numeric,
    uzmobile_load numeric,
    uzmobile_ping text,
    ums_download numeric,
    ums_load numeric,
    ums_ping text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.sales_points OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.sales_points TO gpadmin;




 insert into dictionary.sales_points
 select * from sales_points_old;
 analyze dictionary.sales_points;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.sales_reestr
 ALTER TABLE dictionary.sales_reestr RENAME TO sales_reestr_old;
 




CREATE TABLE dictionary.sales_reestr (
    id integer,
    time_start timestamp without time zone,
    time_execute timestamp without time zone,
    mail text,
    name text,
    region text,
    date_current date,
    sales_point text,
    sales_name text,
    contact text,
    l1 text,
    l2 text,
    l3 text,
    l4 text,
    l5 text,
    plastic text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.sales_reestr OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.sales_reestr TO gpadmin;




 insert into dictionary.sales_reestr
 select * from sales_reestr_old;
 analyze dictionary.sales_reestr;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.service
 ALTER TABLE dictionary.service RENAME TO service_old;
 




CREATE TABLE dictionary.service (
    id integer,
    service_group_id integer,
    service_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.service OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.service TO gpadmin;




 insert into dictionary.service
 select * from service_old;
 analyze dictionary.service;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.service_error
 ALTER TABLE dictionary.service_error RENAME TO service_error_old;
 




CREATE TABLE dictionary.service_error (
    id integer,
    service_id integer,
    error_description text,
    error_description_ru text,
    error_description_uz text,
    error_group text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.service_error OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.service_error TO gpadmin;




 insert into dictionary.service_error
 select * from service_error_old;
 analyze dictionary.service_error;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.service_group
 ALTER TABLE dictionary.service_group RENAME TO service_group_old;
 




CREATE TABLE dictionary.service_group (
    id integer,
    service_group_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.service_group OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.service_group TO gpadmin;




 insert into dictionary.service_group
 select * from service_group_old;
 analyze dictionary.service_group;
end;
$$



DO
$$
begin
 -- Таблица справочник, но слишком большая dictionary.user_level
 ALTER TABLE dictionary.user_level RENAME TO user_level_old;
 




CREATE TABLE dictionary.user_level (
    id integer,
    code text,
    name text,
    description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE dictionary.user_level OWNER TO gpadmin;


GRANT ALL ON TABLE dictionary.user_level TO gpadmin;




 insert into dictionary.user_level
 select * from user_level_old;
 analyze dictionary.user_level;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__asterisk.agent_groups
 ALTER TABLE ods__asterisk.agent_groups RENAME TO agent_groups_old;
 




CREATE TABLE ods__asterisk.agent_groups (
    id integer,
    agent_id integer,
    "group_id" integer,
    group_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__asterisk.agent_groups OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.agent_groups TO gpadmin;




 insert into ods__asterisk.agent_groups
 select * from agent_groups_old;
 analyze ods__asterisk.agent_groups;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__asterisk.agent_queues
 ALTER TABLE ods__asterisk.agent_queues RENAME TO agent_queues_old;
 




CREATE TABLE ods__asterisk.agent_queues (
    id bigint,
    agent_id integer,
    queue_id integer,
    queue_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__asterisk.agent_queues OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.agent_queues TO gpadmin;




 insert into ods__asterisk.agent_queues
 select * from agent_queues_old;
 analyze ods__asterisk.agent_queues;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__asterisk.agents
 ALTER TABLE ods__asterisk.agents RENAME TO agents_old;
 




CREATE TABLE ods__asterisk.agents (
    id integer,
    account_active boolean,
    extension_number text,
    first_name text,
    last_name text,
    international_calls_allowed boolean,
    status_id smallint,
    email text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__asterisk.agents OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.agents TO gpadmin;




 insert into ods__asterisk.agents
 select * from agents_old;
 analyze ods__asterisk.agents;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__asterisk.agents_timetable
 ALTER TABLE ods__asterisk.agents_timetable RENAME TO agents_timetable_old;
 




CREATE TABLE ods__asterisk.agents_timetable (
    id bigint,
    agent_id integer,
    timetable_date date,
    work_time jsonb,
    pause_time jsonb,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__asterisk.agents_timetable OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.agents_timetable TO gpadmin;




 insert into ods__asterisk.agents_timetable
 select * from agents_timetable_old;
 analyze ods__asterisk.agents_timetable;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__asterisk.auto_dial_from_number
 ALTER TABLE ods__asterisk.auto_dial_from_number RENAME TO auto_dial_from_number_old;
 




CREATE TABLE ods__asterisk.auto_dial_from_number (
    id bigint,
    phone text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__asterisk.auto_dial_from_number OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.auto_dial_from_number TO gpadmin;




 insert into ods__asterisk.auto_dial_from_number
 select * from auto_dial_from_number_old;
 analyze ods__asterisk.auto_dial_from_number;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__asterisk.auto_dial_from_numbers_queue
 ALTER TABLE ods__asterisk.auto_dial_from_numbers_queue RENAME TO auto_dial_from_numbers_queue_old;
 




CREATE TABLE ods__asterisk.auto_dial_from_numbers_queue (
    id bigint,
    queue_name text,
    from_number_id bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__asterisk.auto_dial_from_numbers_queue OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.auto_dial_from_numbers_queue TO gpadmin;




 insert into ods__asterisk.auto_dial_from_numbers_queue
 select * from auto_dial_from_numbers_queue_old;
 analyze ods__asterisk.auto_dial_from_numbers_queue;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__asterisk.auto_dial_outbound_campaign
 ALTER TABLE ods__asterisk.auto_dial_outbound_campaign RENAME TO auto_dial_outbound_campaign_old;
 




CREATE TABLE ods__asterisk.auto_dial_outbound_campaign (
    id integer,
    campaign_type text,
    campaign_id text,
    data_set_id uuid,
    name text,
    status text,
    created_date timestamp with time zone,
    created_by text,
    call_complete_count integer,
    call_total_count integer,
    recall_count integer,
    time_between_recall integer,
    queue_name text,
    agent_occupation_coefficient numeric,
    dial_type text,
    handling_agent_percentage numeric,
    working_start_time text,
    working_end_time text,
    knowledge_base_url text,
    max_waiting_time integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__asterisk.auto_dial_outbound_campaign OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.auto_dial_outbound_campaign TO gpadmin;




 insert into ods__asterisk.auto_dial_outbound_campaign
 select * from auto_dial_outbound_campaign_old;
 analyze ods__asterisk.auto_dial_outbound_campaign;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__asterisk.auto_dial_outbound_campaign_data_set уникальное поле:['id'] строк:3011136
 ALTER TABLE ods__asterisk.auto_dial_outbound_campaign_data_set RENAME TO auto_dial_outbound_campaign_data_set_old;
 




CREATE TABLE ods__asterisk.auto_dial_outbound_campaign_data_set (
    id integer,
    data_set_id uuid,
    campaign_id text,
    object text,
    object_access_counter integer,
    last_time_object_access timestamp without time zone,
    state text,
    dial_status text,
    schedule_access_object_time timestamp without time zone,
    additional_id text,
    recipient_unique_id text,
    agent_unique_id text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__asterisk.auto_dial_outbound_campaign_data_set OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.auto_dial_outbound_campaign_data_set TO gpadmin;




 insert into ods__asterisk.auto_dial_outbound_campaign_data_set
 select * from auto_dial_outbound_campaign_data_set_old;
 analyze ods__asterisk.auto_dial_outbound_campaign_data_set;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__asterisk.groups
 ALTER TABLE ods__asterisk.groups RENAME TO groups_old;
 




CREATE TABLE ods__asterisk.groups (
    "group_id" integer,
    name text,
    permissions text,
    description text,
    supervisor_id integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__asterisk.groups OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.groups TO gpadmin;




 insert into ods__asterisk.groups
 select * from groups_old;
 analyze ods__asterisk.groups;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__asterisk.queue_log уникальное поле:['id'] строк:7269470
 ALTER TABLE ods__asterisk.queue_log RENAME TO queue_log_old;
 




CREATE TABLE ods__asterisk.queue_log (
    id bigint,
    "time" timestamp with time zone,
    call_id text,
    queue_name text,
    agent text,
    event text,
    data text,
    data1 text,
    data2 text,
    data3 text,
    data4 text,
    data5 text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__asterisk.queue_log OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.queue_log TO gpadmin;




 insert into ods__asterisk.queue_log
 select * from queue_log_old;
 analyze ods__asterisk.queue_log;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_accumulator.request_history
 ALTER TABLE ods__avo_accumulator.request_history RENAME TO request_history_old;
 




CREATE TABLE ods__avo_accumulator.request_history (
    id uuid,
    accum_id uuid,
    ref_id text,
    ref_type text,
    accum_code text,
    fact_value bigint,
    current_value bigint,
    value bigint,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    trx_id uuid,
    from_time timestamp with time zone,
    to_time timestamp with time zone,
    ext_trx_id text,
    force boolean,
    current_limit bigint,
    action text,
    operations text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_accumulator.request_history OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_accumulator.request_history TO gpadmin;




 insert into ods__avo_accumulator.request_history
 select * from request_history_old;
 analyze ods__avo_accumulator.request_history;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__avo_attempts.attempt_history уникальное поле:['record_hash'] строк:11435866
 ALTER TABLE ods__avo_attempts.attempt_history RENAME TO attempt_history_old;
 




CREATE TABLE ods__avo_attempts.attempt_history (
    attempt_id uuid,
    revision_id bigint,
    revision_type smallint,
    "timestamp" bigint,
    aml_state text,
    fail_reason text,
    is_passed boolean,
    mode text,
    photo_url text,
    provider_task_id text,
    role text,
    step_passed text,
    user_id uuid,
    user_name text,
    verification_state text,
    expiry_at timestamp with time zone,
    completed_at timestamp with time zone,
    person_id bigint,
    status text,
    record_hash uuid,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (record_hash);


ALTER TABLE ods__avo_attempts.attempt_history OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_attempts.attempt_history TO gpadmin;




 insert into ods__avo_attempts.attempt_history
 select * from attempt_history_old;
 analyze ods__avo_attempts.attempt_history;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_automats.automat
 ALTER TABLE ods__avo_automats.automat RENAME TO automat_old;
 




CREATE TABLE ods__avo_automats.automat (
    id text,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    address_ru text,
    address_uz text,
    available_count integer,
    bad_count integer,
    geo_position text,
    status text,
    sw_version text,
    work_mode text,
    deleted boolean,
    type text,
    name_ru text,
    name_uz text,
    description_ru text,
    description_uz text,
    work_time text,
    nearest_points_map text,
    warehouse_id uuid,
    stock_account_nr_a text,
    stock_account_nr_b text,
    version bigint,
    images jsonb,
    status_details jsonb,
    status_details_date timestamp with time zone,
    nearest_points_list_ru jsonb,
    nearest_points_list_uz jsonb,
    tags jsonb,
    playlist_id text,
    services jsonb,
    software_version_id text,
    visible_on_map boolean,
    container_details jsonb,
    status_code text,
    consecutive_ping_amount integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_automats.automat OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_automats.automat TO gpadmin;




 insert into ods__avo_automats.automat
 select * from automat_old;
 analyze ods__avo_automats.automat;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_banners.banners
 ALTER TABLE ods__avo_banners.banners RENAME TO banners_old;
 




CREATE TABLE ods__avo_banners.banners (
    id text,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    is_active boolean,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    chapter text,
    order_id integer,
    data jsonb,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_banners.banners OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_banners.banners TO gpadmin;




 insert into ods__avo_banners.banners
 select * from banners_old;
 analyze ods__avo_banners.banners;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_campaigns.participants
 ALTER TABLE ods__avo_campaigns.participants RENAME TO participants_old;
 




CREATE TABLE ods__avo_campaigns.participants (
    id uuid,
    campaign_id text,
    participant_id text,
    participant_status text,
    created_date timestamp with time zone,
    created_by text,
    last_modified_date timestamp with time zone,
    last_modified_by text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_campaigns.participants OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_campaigns.participants TO gpadmin;




 insert into ods__avo_campaigns.participants
 select * from participants_old;
 analyze ods__avo_campaigns.participants;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_cards.cards
 ALTER TABLE ods__avo_cards.cards RENAME TO cards_old;
 




CREATE TABLE ods__avo_cards.cards (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    user_id uuid,
    product_type text,
    product_id text,
    masked_card_number text,
    encrypted_card_id text,
    currency text,
    expiry_date text,
    card_holder_name text,
    payment_system text,
    status text,
    pin_status text,
    card_name text,
    bank_id uuid,
    card_color text,
    card_order integer,
    is_accounted boolean,
    processing_system text,
    card_number text,
    hash text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_cards.cards OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_cards.cards TO gpadmin;




 insert into ods__avo_cards.cards
 select * from cards_old;
 analyze ods__avo_cards.cards;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__avo_device_manager.notify_tokens уникальное поле:['id'] строк:1186924
 ALTER TABLE ods__avo_device_manager.notify_tokens RENAME TO notify_tokens_old;
 




CREATE TABLE ods__avo_device_manager.notify_tokens (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    user_device_id uuid,
    player_id text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__avo_device_manager.notify_tokens OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_device_manager.notify_tokens TO gpadmin;




 insert into ods__avo_device_manager.notify_tokens
 select * from notify_tokens_old;
 analyze ods__avo_device_manager.notify_tokens;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__avo_device_manager.user_devices уникальное поле:['id'] строк:1541564
 ALTER TABLE ods__avo_device_manager.user_devices RENAME TO user_devices_old;
 




CREATE TABLE ods__avo_device_manager.user_devices (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    device_id uuid,
    user_id uuid,
    is_trusted boolean,
    is_blocked boolean,
    is_active boolean,
    latest_use_date timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__avo_device_manager.user_devices OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_device_manager.user_devices TO gpadmin;




 insert into ods__avo_device_manager.user_devices
 select * from user_devices_old;
 analyze ods__avo_device_manager.user_devices;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_internal_cards.card
 ALTER TABLE ods__avo_internal_cards.card RENAME TO card_old;
 




CREATE TABLE ods__avo_internal_cards.card (
    id text,
    created_date timestamp with time zone,
    last_modified_date timestamp with time zone,
    last_modified_by text,
    created_by text,
    account_nr text,
    card_number text,
    card_order integer,
    is_accounted boolean,
    is_mine boolean,
    bank_id uuid,
    user_id uuid,
    card_color text,
    card_holder_name text,
    card_name text,
    currency text,
    encrypted_card_id text,
    expiry_date text,
    hash text,
    masked_card_number text,
    payment_system text,
    pin_status text,
    processing_system text,
    product_id text,
    product_type text,
    status text,
    card_type text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_internal_cards.card OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_internal_cards.card TO gpadmin;




 insert into ods__avo_internal_cards.card
 select * from card_old;
 analyze ods__avo_internal_cards.card;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__avo_invitation_manager.invitation уникальное поле:['updated_at'] строк:3048537
 ALTER TABLE ods__avo_invitation_manager.invitation RENAME TO invitation_old;
 




CREATE TABLE ods__avo_invitation_manager.invitation (
    invitation_id uuid,
    sender text,
    receiver text,
    hold_value bigint,
    hold_currency text,
    expiry_at timestamp with time zone,
    invitation_type text,
    invitation_status text,
    status_reason text,
    message text,
    created_at timestamp with time zone,
    created_by text,
    updated_at timestamp with time zone,
    updated_by text,
    hold_id text,
    reward_value bigint,
    reward_currency text,
    welcome_value bigint,
    welcome_currency text,
    sender_id uuid,
    sender_bonus_acc text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (updated_at);


ALTER TABLE ods__avo_invitation_manager.invitation OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_invitation_manager.invitation TO gpadmin;




 insert into ods__avo_invitation_manager.invitation
 select * from invitation_old;
 analyze ods__avo_invitation_manager.invitation;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_localizer.translations
 ALTER TABLE ods__avo_localizer.translations RENAME TO translations_old;
 




CREATE TABLE ods__avo_localizer.translations (
    code text,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    text_ru text,
    text_uz text,
    purpose text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_localizer.translations OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_localizer.translations TO gpadmin;




 insert into ods__avo_localizer.translations
 select * from translations_old;
 analyze ods__avo_localizer.translations;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_marketplace.categories
 ALTER TABLE ods__avo_marketplace.categories RENAME TO categories_old;
 




CREATE TABLE ods__avo_marketplace.categories (
    id uuid,
    label_ru text,
    label_uz text,
    image_url text,
    category_order bigint,
    is_enabled boolean,
    created_date timestamp with time zone,
    last_modified_date timestamp with time zone,
    created_by text,
    last_modified_by text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_marketplace.categories OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_marketplace.categories TO gpadmin;




 insert into ods__avo_marketplace.categories
 select * from categories_old;
 analyze ods__avo_marketplace.categories;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_marketplace.merchants
 ALTER TABLE ods__avo_marketplace.merchants RENAME TO merchants_old;
 




CREATE TABLE ods__avo_marketplace.merchants (
    id uuid,
    original_id text,
    aggregator_id text,
    label_ru text,
    label_uz text,
    label_short_ru text,
    label_short_uz text,
    image_url text,
    merchant_order bigint,
    is_enabled boolean,
    is_updated boolean,
    category_id uuid,
    created_date timestamp with time zone,
    last_modified_date timestamp with time zone,
    created_by text,
    last_modified_by text,
    is_deleted boolean,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_marketplace.merchants OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_marketplace.merchants TO gpadmin;




 insert into ods__avo_marketplace.merchants
 select * from merchants_old;
 analyze ods__avo_marketplace.merchants;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_marketplace.services
 ALTER TABLE ods__avo_marketplace.services RENAME TO services_old;
 




CREATE TABLE ods__avo_marketplace.services (
    id uuid,
    original_id text,
    label_ru text,
    label_uz text,
    service_order bigint,
    child_id text,
    is_enabled boolean,
    is_updated boolean,
    min_amount bigint,
    max_amount bigint,
    service_price bigint,
    fee_flat bigint,
    fee_percentage numeric,
    agent_commission numeric,
    aggregator_commission_sum numeric,
    service_commission numeric,
    service_commission_sum numeric,
    merchant_id uuid,
    created_date timestamp with time zone,
    last_modified_date timestamp with time zone,
    created_by text,
    last_modified_by text,
    is_quasicash boolean,
    is_deleted boolean,
    compensation_spec_id text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_marketplace.services OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_marketplace.services TO gpadmin;




 insert into ods__avo_marketplace.services
 select * from services_old;
 analyze ods__avo_marketplace.services;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_offers.cl_transaction
 ALTER TABLE ods__avo_offers.cl_transaction RENAME TO cl_transaction_old;
 




CREATE TABLE ods__avo_offers.cl_transaction (
    id text,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    applied_cl bigint,
    cl_application_status text,
    cl_processed_at timestamp with time zone,
    cl_transaction_type text,
    current_cl bigint,
    offers_ids text,
    requested_cl bigint,
    user_id uuid,
    command_id uuid,
    error_code text,
    is_cl_restore boolean,
    product_id text,
    reference_id text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_offers.cl_transaction OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_offers.cl_transaction TO gpadmin;




 insert into ods__avo_offers.cl_transaction
 select * from cl_transaction_old;
 analyze ods__avo_offers.cl_transaction;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__avo_offers.command уникальное поле:['id'] строк:1004477
 ALTER TABLE ods__avo_offers.command RENAME TO command_old;
 




CREATE TABLE ods__avo_offers.command (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    command_set_id uuid,
    command_type text,
    credit_limit_abs_value bigint,
    status text,
    transaction_id text,
    user_id uuid,
    processed_at timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__avo_offers.command OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_offers.command TO gpadmin;




 insert into ods__avo_offers.command
 select * from command_old;
 analyze ods__avo_offers.command;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_offers.offer
 ALTER TABLE ods__avo_offers.offer RENAME TO offer_old;
 




CREATE TABLE ods__avo_offers.offer (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    cl_value bigint,
    cl_value_type text,
    condition text,
    ref_instance text,
    status text,
    transaction_id text,
    command_id uuid,
    accepted_at timestamp with time zone,
    completed_at timestamp with time zone,
    valid_to timestamp with time zone,
    is_cl_restore boolean,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_offers.offer OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_offers.offer TO gpadmin;




 insert into ods__avo_offers.offer
 select * from offer_old;
 analyze ods__avo_offers.offer;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__avo_otp_manager.otp уникальное поле:['otp_id'] строк:7016143
 ALTER TABLE ods__avo_otp_manager.otp RENAME TO otp_old;
 




CREATE TABLE ods__avo_otp_manager.otp (
    otp_id uuid,
    id text,
    id_type text,
    notification_code text,
    code text,
    status text,
    created_at timestamp with time zone,
    last_attempt_at timestamp with time zone,
    expiry_at timestamp with time zone,
    attempts integer,
    payload jsonb,
    params jsonb,
    lang text,
    resend_count integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (otp_id);


ALTER TABLE ods__avo_otp_manager.otp OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_otp_manager.otp TO gpadmin;




 insert into ods__avo_otp_manager.otp
 select * from otp_old;
 analyze ods__avo_otp_manager.otp;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_pickouts.pickout
 ALTER TABLE ods__avo_pickouts.pickout RENAME TO pickout_old;
 




CREATE TABLE ods__avo_pickouts.pickout (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    card_id text,
    automat_id text,
    status text,
    user_id uuid,
    person_id bigint,
    automat_type text,
    fail_reason text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_pickouts.pickout OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_pickouts.pickout TO gpadmin;




 insert into ods__avo_pickouts.pickout
 select * from pickout_old;
 analyze ods__avo_pickouts.pickout;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_registries.registries
 ALTER TABLE ods__avo_registries.registries RENAME TO registries_old;
 




CREATE TABLE ods__avo_registries.registries (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    file_name text,
    provider_agreement_id text,
    provider_id text,
    rec_count_error integer,
    rec_count_man_fix integer,
    rec_count_total integer,
    settlement_date timestamp without time zone,
    rec_count_success integer,
    total_amount_credit_success bigint,
    total_amount_debit_success bigint,
    total_amount_credit_failed bigint,
    total_amount_debit_failed bigint,
    status text,
    type text,
    rec_count_debit bigint,
    rec_count_credit bigint,
    clearing_status text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_registries.registries OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_registries.registries TO gpadmin;




 insert into ods__avo_registries.registries
 select * from registries_old;
 analyze ods__avo_registries.registries;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_registries.registry_files
 ALTER TABLE ods__avo_registries.registry_files RENAME TO registry_files_old;
 




CREATE TABLE ods__avo_registries.registry_files (
    file_name text,
    processed_date timestamp with time zone,
    provider_id text,
    status text,
    ordinal integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_registries.registry_files OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_registries.registry_files TO gpadmin;




 insert into ods__avo_registries.registry_files
 select * from registry_files_old;
 analyze ods__avo_registries.registry_files;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_stop_list.villain
 ALTER TABLE ods__avo_stop_list.villain RENAME TO villain_old;
 




CREATE TABLE ods__avo_stop_list.villain (
    id uuid,
    record_processing_status text,
    additional_info text,
    address text,
    alternative_names text,
    birth_date timestamp without time zone,
    birth_place text,
    doc_id text,
    is_active boolean,
    order_no bigint,
    pinfl text,
    villain_type text,
    created_at timestamp with time zone,
    created_by text,
    updated_at timestamp with time zone,
    updated_by text,
    lat_names jsonb,
    original_names jsonb,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_stop_list.villain OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_stop_list.villain TO gpadmin;




 insert into ods__avo_stop_list.villain
 select * from villain_old;
 analyze ods__avo_stop_list.villain;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_stories.stories
 ALTER TABLE ods__avo_stories.stories RENAME TO stories_old;
 




CREATE TABLE ods__avo_stories.stories (
    id text,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    is_active boolean,
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    title text,
    campaign_id text,
    image text,
    order_id integer,
    chapter text,
    channels jsonb,
    pages jsonb,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_stories.stories OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_stories.stories TO gpadmin;




 insert into ods__avo_stories.stories
 select * from stories_old;
 analyze ods__avo_stories.stories;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_tuner.toggles
 ALTER TABLE ods__avo_tuner.toggles RENAME TO toggles_old;
 




CREATE TABLE ods__avo_tuner.toggles (
    id text,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    type text,
    name text,
    description text,
    value boolean,
    author text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_tuner.toggles OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_tuner.toggles TO gpadmin;




 insert into ods__avo_tuner.toggles
 select * from toggles_old;
 analyze ods__avo_tuner.toggles;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__avo_users.app_users уникальное поле:['id'] строк:1334986
 ALTER TABLE ods__avo_users.app_users RENAME TO app_users_old;
 




CREATE TABLE ods__avo_users.app_users (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    username text,
    password text,
    pinfl text,
    email text,
    first_name text,
    last_name text,
    nickname text,
    lang text,
    avatar text,
    birth_date date,
    status text,
    verification_state text,
    person_id bigint,
    pwd_approve boolean,
    level text,
    doc_id text,
    second_name text,
    first_name_cleaned text,
    last_name_cleaned text,
    second_name_cleaned text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1')
 DISTRIBUTED BY (id);


ALTER TABLE ods__avo_users.app_users OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_users.app_users TO gpadmin;




 insert into ods__avo_users.app_users
 select * from app_users_old;
 analyze ods__avo_users.app_users;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__avo_users.sessions уникальное поле:['id'] строк:1192597
 ALTER TABLE ods__avo_users.sessions RENAME TO sessions_old;
 




CREATE TABLE ods__avo_users.sessions (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    app_user_id uuid,
    device_id uuid,
    session_id uuid,
    jwt_id uuid,
    device_type text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__avo_users.sessions OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_users.sessions TO gpadmin;




 insert into ods__avo_users.sessions
 select * from sessions_old;
 analyze ods__avo_users.sessions;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_warehouses.container
 ALTER TABLE ods__avo_warehouses.container RENAME TO container_old;
 




CREATE TABLE ods__avo_warehouses.container (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    warehouse_id uuid,
    type text,
    status text,
    cards_qty integer,
    code text,
    position_id text,
    position_type text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_warehouses.container OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_warehouses.container TO gpadmin;




 insert into ods__avo_warehouses.container
 select * from container_old;
 analyze ods__avo_warehouses.container;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_warehouses.warehouse
 ALTER TABLE ods__avo_warehouses.warehouse RENAME TO warehouse_old;
 




CREATE TABLE ods__avo_warehouses.warehouse (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    type text,
    status text,
    region text,
    name_ru text,
    name_uz text,
    address_ru text,
    address_uz text,
    description_ru text,
    description_uz text,
    geo_position text,
    stock_accout_nr_a text,
    stock_accout_nr_b text,
    stock_accout_nr_c text,
    stock_accout_nr_d text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_warehouses.warehouse OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_warehouses.warehouse TO gpadmin;




 insert into ods__avo_warehouses.warehouse
 select * from warehouse_old;
 analyze ods__avo_warehouses.warehouse;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_warehouses_expeditions.expedition
 ALTER TABLE ods__avo_warehouses_expeditions.expedition RENAME TO expedition_old;
 




CREATE TABLE ods__avo_warehouses_expeditions.expedition (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    status text,
    automat_id text,
    reject_bag_used text,
    manifest_id uuid,
    finished_date timestamp with time zone,
    warehouse_id uuid,
    cassettes_in jsonb,
    cassettes_out jsonb,
    transfer_ids jsonb,
    cardtransfer_ids jsonb,
    reject_cards jsonb,
    cassettes_inserted jsonb,
    cassettes_extracted jsonb,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_warehouses_expeditions.expedition OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_warehouses_expeditions.expedition TO gpadmin;




 insert into ods__avo_warehouses_expeditions.expedition
 select * from expedition_old;
 analyze ods__avo_warehouses_expeditions.expedition;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__avo_warehouses_transfers.card_transfer
 ALTER TABLE ods__avo_warehouses_transfers.card_transfer RENAME TO card_transfer_old;
 




CREATE TABLE ods__avo_warehouses_transfers.card_transfer (
    id uuid,
    created_by text,
    created_date timestamp with time zone,
    last_modified_by text,
    last_modified_date timestamp with time zone,
    type text,
    cards text[],
    warehouse_id uuid,
    finished_date timestamp with time zone,
    cards_quantity integer,
    src_transfer_side_type text,
    src_side_id text,
    dest_transfer_side_type text,
    dest_side_id text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__avo_warehouses_transfers.card_transfer OWNER TO gpadmin;


GRANT ALL ON TABLE ods__avo_warehouses_transfers.card_transfer TO gpadmin;




 insert into ods__avo_warehouses_transfers.card_transfer
 select * from card_transfer_old;
 analyze ods__avo_warehouses_transfers.card_transfer;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__crm.reasons1
 ALTER TABLE ods__crm.reasons1 RENAME TO reasons1_old;
 




CREATE TABLE ods__crm.reasons1 (
    id uuid,
    name text,
    kb_url text,
    archival boolean,
    created_by text,
    updated_by text,
    created_dt timestamp with time zone,
    updated_dt timestamp with time zone,
    type text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__crm.reasons1 OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.reasons1 TO gpadmin;




 insert into ods__crm.reasons1
 select * from reasons1_old;
 analyze ods__crm.reasons1;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__crm.reasons2
 ALTER TABLE ods__crm.reasons2 RENAME TO reasons2_old;
 




CREATE TABLE ods__crm.reasons2 (
    id uuid,
    name text,
    kb_url text,
    archival boolean,
    created_by text,
    updated_by text,
    created_dt timestamp with time zone,
    updated_dt timestamp with time zone,
    type text,
    reason1_id uuid,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__crm.reasons2 OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.reasons2 TO gpadmin;




 insert into ods__crm.reasons2
 select * from reasons2_old;
 analyze ods__crm.reasons2;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__crm.reasons3
 ALTER TABLE ods__crm.reasons3 RENAME TO reasons3_old;
 




CREATE TABLE ods__crm.reasons3 (
    id uuid,
    name text,
    kb_url text,
    archival boolean,
    created_by text,
    updated_by text,
    created_dt timestamp with time zone,
    updated_dt timestamp with time zone,
    type text,
    reason2_id uuid,
    template_id integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__crm.reasons3 OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.reasons3 TO gpadmin;




 insert into ods__crm.reasons3
 select * from reasons3_old;
 analyze ods__crm.reasons3;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__crm.service_requests уникальное поле:['id'] строк:1165306
 ALTER TABLE ods__crm.service_requests RENAME TO service_requests_old;
 




CREATE TABLE ods__crm.service_requests (
    id bigint,
    created_dt timestamp with time zone,
    created_by text,
    channel text,
    direction text,
    contact text,
    id_type text,
    id_value text,
    status text,
    call_record_url text,
    is_repeated boolean,
    call_id text,
    issue_id text,
    comment text,
    username text,
    reason1_id uuid,
    reason2_id uuid,
    reason3_id uuid,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__crm.service_requests OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.service_requests TO gpadmin;




 insert into ods__crm.service_requests
 select * from service_requests_old;
 analyze ods__crm.service_requests;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__crm.template_answers уникальное поле:['id'] строк:2858248
 ALTER TABLE ods__crm.template_answers RENAME TO template_answers_old;
 




CREATE TABLE ods__crm.template_answers (
    id bigint,
    value text,
    question_id integer,
    service_request_id bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__crm.template_answers OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.template_answers TO gpadmin;




 insert into ods__crm.template_answers
 select * from template_answers_old;
 analyze ods__crm.template_answers;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__crm.template_options
 ALTER TABLE ods__crm.template_options RENAME TO template_options_old;
 




CREATE TABLE ods__crm.template_options (
    id integer,
    value text,
    question_id integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__crm.template_options OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.template_options TO gpadmin;




 insert into ods__crm.template_options
 select * from template_options_old;
 analyze ods__crm.template_options;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__crm.template_question_types
 ALTER TABLE ods__crm.template_question_types RENAME TO template_question_types_old;
 




CREATE TABLE ods__crm.template_question_types (
    id integer,
    name text,
    archived boolean,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__crm.template_question_types OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.template_question_types TO gpadmin;




 insert into ods__crm.template_question_types
 select * from template_question_types_old;
 analyze ods__crm.template_question_types;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__crm.template_questions
 ALTER TABLE ods__crm.template_questions RENAME TO template_questions_old;
 




CREATE TABLE ods__crm.template_questions (
    id integer,
    type_id integer,
    template_id integer,
    name text,
    limits bigint,
    archived boolean,
    created_dt timestamp with time zone,
    created_by text,
    updated_dt timestamp with time zone,
    updated_by text,
    required boolean,
    hint text,
    default_option_id integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__crm.template_questions OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.template_questions TO gpadmin;




 insert into ods__crm.template_questions
 select * from template_questions_old;
 analyze ods__crm.template_questions;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__crm.templates
 ALTER TABLE ods__crm.templates RENAME TO templates_old;
 




CREATE TABLE ods__crm.templates (
    id integer,
    name text,
    archived boolean,
    created_dt timestamp with time zone,
    created_by text,
    updated_dt timestamp with time zone,
    updated_by text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__crm.templates OWNER TO gpadmin;


GRANT ALL ON TABLE ods__crm.templates TO gpadmin;




 insert into ods__crm.templates
 select * from templates_old;
 analyze ods__crm.templates;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__fido.accounts уникальное поле:['id'] строк:6873827
 ALTER TABLE ods__fido.accounts RENAME TO accounts_old;
 




CREATE TABLE ods__fido.accounts (
    id bigint,
    code text,
    code_filial text,
    name text,
    liability_active text,
    balance_out text,
    sign_registr text,
    saldo_in bigint,
    saldo_out bigint,
    saldo_equival_in bigint,
    saldo_equival_out bigint,
    saldo_unlead bigint,
    turnover_debit bigint,
    turnover_credit bigint,
    turnover_all_debit bigint,
    turnover_all_credit bigint,
    lead_last_date date,
    status text,
    condition text,
    client_code text,
    group_code text,
    acc_external text,
    code_coa text,
    code_currency text,
    a_client_code text,
    a_account_code text,
    owned_filial text,
    subcoa_code text,
    client_id integer,
    date_open date,
    date_validate date,
    date_change_condition date,
    owned_employee integer,
    local_code text,
    product_id integer,
    eqv_turnover_debit bigint,
    eqv_turnover_credit bigint,
    eqv_turnover_all_debit bigint,
    eqv_turnover_all_credit bigint,
    curr_oper_code text,
    curr_oper_subcode text,
    mcc_param_type text,
    ips_param_type text,
    mt_param_type text,
    msfo text,
    display_name text,
    external_product_id text,
    external_product_code integer,
    client_uid integer,
    group_set text,
    category_set smallint,
    security_level smallint,
    branch_id integer,
    cross_acc_id integer,
    cross_code_filial text,
    deal_id integer,
    sb_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__fido.accounts OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.accounts TO gpadmin;




 insert into ods__fido.accounts
 select * from accounts_old;
 analyze ods__fido.accounts;
end;
$$



DO
$$
begin
 -- Таблица без уникального ключа ods__fido.accounts_history уникальное поле:MD5(CONCAT(COALESCE("id", 0), COALESCE("date_modify", '1970-01-01 00:00:00')))::uuid строк:6903535
 ALTER TABLE ods__fido.accounts_history RENAME TO accounts_history_old;
 




CREATE TABLE ods__fido.accounts_history (
    id bigint,
    code_filial text,
    name text,
    liability_active text,
    sign_registr text,
    status text,
    condition text,
    balance_out text,
    date_validate date,
    date_modify timestamp with time zone,
    group_code text,
    a_client_code text,
    a_account_code text,
    owned_filial text,
    subcoa_code text,
    date_change_condition date,
    owned_employee integer,
    local_code text,
    product_id integer,
    date_next date,
    curr_oper_code text,
    curr_oper_subcode text,
    mcc_param_type text,
    ips_param_type text,
    mt_param_type text,
    display_name text,
    external_product_id text,
    external_product_code integer,
    branch_id integer,
    deal_id integer,
    sb_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
,MD5(CONCAT(COALESCE("id", 0), COALESCE("date_modify", '1970-01-01 00:00:00')))::uuid)  
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1)
 DISTRIBUTED (record_hash);


ALTER TABLE ods__fido.accounts_history OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.accounts_history TO gpadmin;




 insert into ods__fido.accounts_history
 select * from accounts_history_old;
 analyze ods__fido.accounts_history;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.acn_contract_subjects
 ALTER TABLE ods__fido.acn_contract_subjects RENAME TO acn_contract_subjects_old;
 




CREATE TABLE ods__fido.acn_contract_subjects (
    subject_id integer,
    filial text,
    contract_id integer,
    id_add_agr integer,
    acc_external text,
    measure_id integer,
    price numeric,
    amount numeric,
    amount_saldo numeric,
    summ numeric,
    summ_saldo numeric,
    "precision" integer,
    created_by integer,
    creation_date timestamp with time zone,
    last_updated_by integer,
    last_update_date timestamp with time zone,
    subject_group_code text,
    material_code text,
    description text,
    type_id smallint,
    material_name text,
    material_detail text,
    id_add_on_subject bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.acn_contract_subjects OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.acn_contract_subjects TO gpadmin;




 insert into ods__fido.acn_contract_subjects
 select * from acn_contract_subjects_old;
 analyze ods__fido.acn_contract_subjects;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.acn_contracts
 ALTER TABLE ods__fido.acn_contracts RENAME TO acn_contracts_old;
 




CREATE TABLE ods__fido.acn_contracts (
    contract_id integer,
    filial text,
    contractor_id integer,
    reg_number text,
    reg_date date,
    group_code text,
    date_beg date,
    date_end date,
    acc_rep text,
    acc_pay text,
    summ numeric,
    nds_pers numeric,
    summ_paid numeric,
    summ_saldo numeric,
    description text,
    state_id integer,
    state_reason_text text,
    created_by integer,
    creation_date timestamp with time zone,
    last_updated_by integer,
    last_update_date timestamp with time zone,
    contractor_acc text,
    currency_id text,
    confirm_dt numeric,
    confirm_cr numeric,
    saldo_dt numeric,
    saldo_cr numeric,
    potreb text,
    potreb_type text,
    branch_mfo text,
    type_purchase integer,
    lot_num text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.acn_contracts OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.acn_contracts TO gpadmin;




 insert into ods__fido.acn_contracts
 select * from acn_contracts_old;
 analyze ods__fido.acn_contracts;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.calendar
 ALTER TABLE ods__fido.calendar RENAME TO calendar_old;
 




CREATE TABLE ods__fido.calendar (
    oper_day date,
    day_status smallint,
    prc_invoice_status smallint,
    prc_cover_status smallint,
    confirmed text,
    emp_confirm integer,
    date_confirm timestamp with time zone,
    include_operday text,
    description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.calendar OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.calendar TO gpadmin;




 insert into ods__fido.calendar
 select * from calendar_old;
 analyze ods__fido.calendar;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.client_current
 ALTER TABLE ods__fido.client_current RENAME TO client_current_old;
 




CREATE TABLE ods__fido.client_current (
    code text,
    typeof text,
    subject text,
    inn text,
    condition text,
    resident_code text,
    country_code text,
    property_form_code text,
    operator_code integer,
    date_validate date,
    date_modify timestamp with time zone,
    code_filial text,
    a_client_code text,
    id bigint,
    region_code text,
    district_code text,
    date_end date,
    date_open date,
    date_change_condition date,
    cell_number text,
    web text,
    notice text,
    resident_type text,
    swift_id text,
    client_uid bigint,
    coato text,
    attracted_client text,
    user_bonus bigint,
    task_code text,
    module_code text,
    identification_type text,
    date_deactivate date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.client_current OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.client_current TO gpadmin;




 insert into ods__fido.client_current
 select * from client_current_old;
 analyze ods__fido.client_current;
end;
$$



DO
$$
begin
 -- Таблица без уникального ключа ods__fido.client_history уникальное поле:MD5(CONCAT(COALESCE("code", ''), COALESCE("date_validate", '1970-01-01')))::uuid строк:1103650
 ALTER TABLE ods__fido.client_history RENAME TO client_history_old;
 




CREATE TABLE ods__fido.client_history (
    code text,
    typeof text,
    subject text,
    inn text,
    condition text,
    resident_code text,
    country_code text,
    property_form_code text,
    operator_code integer,
    date_validate date,
    date_modify timestamp with time zone,
    code_filial text,
    a_client_code text,
    id bigint,
    region_code text,
    district_code text,
    date_end date,
    date_open date,
    date_change_condition date,
    cell_number text,
    web text,
    notice text,
    resident_type text,
    swift_id text,
    client_uid bigint,
    coato text,
    attracted_client text,
    user_bonus bigint,
    task_code text,
    module_code text,
    identification_type text,
    date_deactivate date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
,MD5(CONCAT(COALESCE("code", ''), COALESCE("date_validate", '1970-01-01')))::uuid)  
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1)
 DISTRIBUTED (record_hash);


ALTER TABLE ods__fido.client_history OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.client_history TO gpadmin;




 insert into ods__fido.client_history
 select * from client_history_old;
 analyze ods__fido.client_history;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.day_operational
 ALTER TABLE ods__fido.day_operational RENAME TO day_operational_old;
 




CREATE TABLE ods__fido.day_operational (
    prev_oper_day date,
    oper_day date,
    next_oper_day date,
    day_status smallint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.day_operational OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.day_operational TO gpadmin;




 insert into ods__fido.day_operational
 select * from day_operational_old;
 analyze ods__fido.day_operational;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.dep_accounts
 ALTER TABLE ods__fido.dep_accounts RENAME TO dep_accounts_old;
 




CREATE TABLE ods__fido.dep_accounts (
    contract_id integer,
    account_type smallint,
    date_validate date,
    filial_code text,
    account_code text,
    emp_code integer,
    date_modify timestamp with time zone,
    date_next date,
    acc_id integer,
    coa text,
    subcoa_code text,
    branch_id integer,
    local_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.dep_accounts OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.dep_accounts TO gpadmin;




 insert into ods__fido.dep_accounts
 select * from dep_accounts_old;
 analyze ods__fido.dep_accounts;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.dep_accounts_his
 ALTER TABLE ods__fido.dep_accounts_his RENAME TO dep_accounts_his_old;
 




CREATE TABLE ods__fido.dep_accounts_his (
    contract_id integer,
    account_type integer,
    date_validate date,
    filial_code text,
    account_code text,
    emp_code integer,
    date_modify timestamp with time zone,
    action text,
    date_next date,
    acc_id integer,
    coa text,
    subcoa_code text,
    branch_id integer,
    local_code text,
    oper_day date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.dep_accounts_his OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.dep_accounts_his TO gpadmin;




 insert into ods__fido.dep_accounts_his
 select * from dep_accounts_his_old;
 analyze ods__fido.dep_accounts_his;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.dep_contracts
 ALTER TABLE ods__fido.dep_contracts RENAME TO dep_contracts_old;
 




CREATE TABLE ods__fido.dep_contracts (
    id integer,
    oper_date date,
    filial_code text,
    parent_id integer,
    child_id integer,
    account_layouts integer,
    action_resource text,
    currency_type text,
    client_type text,
    deposit_type integer,
    client_code text,
    contract_name text,
    contract_number text,
    contract_date date,
    date_begin date,
    date_end date,
    date_end_fact date,
    currency_code text,
    summa bigint,
    count_days integer,
    min_summa_calc_percent integer,
    state text,
    emp_code integer,
    date_modify timestamp with time zone,
    percent_min_summa numeric,
    loan_id integer,
    pledge text,
    product_type integer,
    client_id integer,
    client_uid integer,
    branch_id integer,
    local_code text,
    hide_contract text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.dep_contracts OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.dep_contracts TO gpadmin;




 insert into ods__fido.dep_contracts
 select * from dep_contracts_old;
 analyze ods__fido.dep_contracts;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.dep_contracts_his
 ALTER TABLE ods__fido.dep_contracts_his RENAME TO dep_contracts_his_old;
 




CREATE TABLE ods__fido.dep_contracts_his (
    id numeric,
    oper_date date,
    filial_code text,
    parent_id numeric,
    child_id numeric,
    account_layouts numeric,
    action_resource text,
    currency_type text,
    client_type text,
    deposit_type numeric,
    client_code text,
    contract_name text,
    contract_number text,
    contract_date date,
    date_begin date,
    date_end date,
    date_end_fact date,
    currency_code text,
    summa numeric,
    count_days numeric,
    min_summa_calc_percent numeric,
    state text,
    emp_code numeric,
    date_modify timestamp with time zone,
    percent_min_summa numeric,
    loan_id numeric,
    pledge text,
    product_type numeric,
    client_id numeric,
    client_uid numeric,
    branch_id numeric,
    local_code text,
    hide_contract text,
    oper_day date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.dep_contracts_his OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.dep_contracts_his TO gpadmin;




 insert into ods__fido.dep_contracts_his
 select * from dep_contracts_his_old;
 analyze ods__fido.dep_contracts_his;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.dwh_coa
 ALTER TABLE ods__fido.dwh_coa RENAME TO dwh_coa_old;
 




CREATE TABLE ods__fido.dwh_coa (
    filial_code text,
    oper_day date,
    coa_code text,
    currency_code text,
    saldo_inm bigint,
    saldo_inp bigint,
    saldo_outm bigint,
    saldo_outp bigint,
    saldo_eqv_inm bigint,
    saldo_eqv_inp bigint,
    saldo_eqv_outm bigint,
    saldo_eqv_outp bigint,
    debit_circulate bigint,
    credit_circulate bigint,
    resident text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.dwh_coa OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.dwh_coa TO gpadmin;




 insert into ods__fido.dwh_coa
 select * from dwh_coa_old;
 analyze ods__fido.dwh_coa;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.dwh_coa_y
 ALTER TABLE ods__fido.dwh_coa_y RENAME TO dwh_coa_y_old;
 




CREATE TABLE ods__fido.dwh_coa_y (
    filial_code text,
    oper_day date,
    coa_code text,
    currency_code text,
    saldo_inm bigint,
    saldo_inp bigint,
    saldo_outm bigint,
    saldo_outp bigint,
    saldo_eqv_inm bigint,
    saldo_eqv_inp bigint,
    saldo_eqv_outm bigint,
    saldo_eqv_outp bigint,
    debit_circulate bigint,
    credit_circulate bigint,
    resident text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.dwh_coa_y OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.dwh_coa_y TO gpadmin;




 insert into ods__fido.dwh_coa_y
 select * from dwh_coa_y_old;
 analyze ods__fido.dwh_coa_y;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.employee_current
 ALTER TABLE ods__fido.employee_current RENAME TO employee_current_old;
 




CREATE TABLE ods__fido.employee_current (
    code integer,
    filial_code text,
    local_code text,
    name text,
    rank_code integer,
    login text,
    nls text,
    condition text,
    operator_code integer,
    date_validate date,
    date_modify timestamp with time zone,
    kadr_id integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.employee_current OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.employee_current TO gpadmin;




 insert into ods__fido.employee_current
 select * from employee_current_old;
 analyze ods__fido.employee_current;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.hr_departments
 ALTER TABLE ods__fido.hr_departments RENAME TO hr_departments_old;
 




CREATE TABLE ods__fido.hr_departments (
    id integer,
    code text,
    parent_code text,
    condition text,
    filial text,
    lev integer,
    order_by integer,
    rep_condition text,
    created_by integer,
    creation_date timestamp with time zone,
    last_updated_by integer,
    last_update_date timestamp with time zone,
    branch_type text,
    branch_code text,
    class_id bigint,
    local_code text,
    branch_id integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.hr_departments OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.hr_departments TO gpadmin;




 insert into ods__fido.hr_departments
 select * from hr_departments_old;
 analyze ods__fido.hr_departments;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.hr_emp_works
 ALTER TABLE ods__fido.hr_emp_works RENAME TO hr_emp_works_old;
 




CREATE TABLE ods__fido.hr_emp_works (
    work_id integer,
    emp_id integer,
    bank_branch_code text,
    code_post_cb text,
    total_bank text,
    begin_date date,
    end_date date,
    end_date_contract date,
    staffing_id integer,
    work_dep text,
    work_post text,
    org_name text,
    org_loc text,
    org_type text,
    ord_code_beg text,
    ord_beg text,
    ord_date_beg date,
    ord_code_end text,
    ord_end text,
    ord_date_end date,
    contract_number text,
    stavka numeric,
    reason_end text,
    reason_end_text text,
    date_ut_km date,
    nom_km text,
    date_kval_cb date,
    nom_kval_cb text,
    date_attes date,
    attestat text,
    for_14_zp text,
    work_now text,
    created_by integer,
    creation_date timestamp with time zone,
    last_updated_by integer,
    last_update_date timestamp with time zone,
    reason_info text,
    reason_order text,
    local_code text,
    branch_id integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.hr_emp_works OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.hr_emp_works TO gpadmin;




 insert into ods__fido.hr_emp_works
 select * from hr_emp_works_old;
 analyze ods__fido.hr_emp_works;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.hr_emps
 ALTER TABLE ods__fido.hr_emps RENAME TO hr_emps_old;
 




CREATE TABLE ods__fido.hr_emps (
    emp_id integer,
    tab_num text,
    cb_id integer,
    filial text,
    filial_old text,
    last_name text,
    first_name text,
    middle_name text,
    date_begin date,
    date_end date,
    staffing_id integer,
    staffing_id_old integer,
    stavka numeric,
    day_work_time text,
    work_type text,
    work_spec text,
    condition text,
    condition_old text,
    inps text,
    inn text,
    nationality text,
    marriage text,
    bank_spec text,
    approval_type text,
    gender text,
    from_bank_flag text,
    birth_date date,
    birth_place text,
    birth_country_code text,
    birth_region_code text,
    birth_district_code text,
    citizenship text,
    citizenship_country text,
    citizenship_region text,
    country_code text,
    region_code text,
    district_code text,
    address text,
    temp_country_code text,
    temp_region_code text,
    temp_district_code text,
    temp_address text,
    temp_begin_date date,
    temp_end_date date,
    education text,
    party text,
    party_date date,
    labor_union_flag text,
    passport_seria text,
    passport_number text,
    passport_issued text,
    passport_date_begin date,
    passport_date_end date,
    passport_ever_flag text,
    pension_age_flag text,
    sience_degree text,
    sience_rank text,
    sience_work text,
    sience_data_nom text,
    conviction_flag text,
    conviction_date_begin date,
    conviction_date_end date,
    conviction_reason text,
    conviction_desc text,
    army_ready_flag text,
    army_group_number text,
    army_sostav text,
    army_office text,
    army_rank text,
    army_card_number text,
    army_spec_flag text,
    army_vus text,
    army_where_when text,
    phone text,
    phone_work text,
    phone_mobil text,
    phone_mobil2 text,
    mail_address text,
    comp_knowledge text,
    comp_knowledge_desc text,
    net_knowledge text,
    net_knowledge_desc text,
    additional_income text,
    additional_income_desc text,
    estate_own text,
    estate_own_desc text,
    do_business_flag text,
    last_work_fired_reason text,
    old_last_name text,
    fio_update_reason text,
    public_work text,
    public_work_desc text,
    created_by text,
    creation_date timestamp with time zone,
    last_updated_by text,
    last_update_date timestamp with time zone,
    local_code text,
    turnstile_id text,
    log_in text,
    arm_id text,
    phone_work_vnu text,
    telegram text,
    branch_id text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.hr_emps OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.hr_emps TO gpadmin;




 insert into ods__fido.hr_emps
 select * from hr_emps_old;
 analyze ods__fido.hr_emps;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.hr_s_departments
 ALTER TABLE ods__fido.hr_s_departments RENAME TO hr_s_departments_old;
 




CREATE TABLE ods__fido.hr_s_departments (
    id integer,
    code text,
    department_name text,
    created_by integer,
    creation_date timestamp with time zone,
    last_updated_by integer,
    last_update_date timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.hr_s_departments OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.hr_s_departments TO gpadmin;




 insert into ods__fido.hr_s_departments
 select * from hr_s_departments_old;
 analyze ods__fido.hr_s_departments;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.hr_staffing
 ALTER TABLE ods__fido.hr_staffing RENAME TO hr_staffing_old;
 




CREATE TABLE ods__fido.hr_staffing (
    staffing_id integer,
    filial text,
    department_code text,
    post_id smallint,
    vacansion_date timestamp with time zone,
    amount numeric,
    rotation_flag text,
    rotation_month smallint,
    state text,
    condition text,
    category_emp text,
    uniform_flag text,
    financial_resp_flag text,
    rep_condition text,
    created_by integer,
    creation_date timestamp with time zone,
    last_updated_by integer,
    last_update_date timestamp with time zone,
    direct_code text,
    post_iabs smallint,
    bases text,
    workly_flag text,
    vacation_flag text,
    increase_percent smallint,
    edit_text text,
    edo_flag text,
    arm_id text,
    class_id integer,
    vacant_days integer,
    related_postvacant_days integer,
    sokr_date date,
    sokr_end_date date,
    sokr_text text,
    deactivation_flag text,
    related_post integer,
    stavka text,
    location text,
    local_code text,
    resp_zone text,
    branch_id integer,
    res_department_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.hr_staffing OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.hr_staffing TO gpadmin;




 insert into ods__fido.hr_staffing
 select * from hr_staffing_old;
 analyze ods__fido.hr_staffing;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.leads
 ALTER TABLE ods__fido.leads RENAME TO leads_old;
 




CREATE TABLE ods__fido.leads (
    doc_id bigint,
    maket_id text,
    code_filial text,
    id bigint,
    doc_numb text,
    doc_date date,
    cl_mfo text,
    cl_acc text,
    cl_inn text,
    cl_name text,
    co_mfo text,
    co_acc text,
    co_inn text,
    co_name text,
    pay_purpose text,
    sum_pay numeric,
    sum_eqv numeric,
    code_document text,
    op_dc smallint,
    trans_id smallint,
    code_currency text,
    sys_birth smallint,
    emp_birth integer,
    sys_id smallint,
    code_emp integer,
    id_parent bigint,
    id_child bigint,
    date_enter timestamp with time zone,
    date_execute timestamp with time zone,
    date_suspend timestamp with time zone,
    date_activ timestamp with time zone,
    sign_origin text,
    sign_internal text,
    sign_balance text,
    code_plan smallint,
    code_rest smallint,
    reason_code integer,
    err_code integer,
    act_id smallint,
    state_id smallint,
    curr_day date,
    sym_id integer,
    task_code integer,
    contract_id integer,
    doc_type_id integer,
    purpose_sub_code text,
    swift_code text,
    partner_status text,
    init_task_code integer,
    date_value timestamp with time zone,
    idnk text,
    deal_id bigint,
    document_id bigint,
    order_num smallint,
    operation_id bigint,
    client_id bigint,
    product_id integer,
    module_code text,
    init_module_code text,
    branch_id integer,
    local_code text,
    sign_inter text,
    cl_pinfl text,
    co_pinfl text,
    swift_reference text,
    calendar_day date,
    is_future text,
    branch_id_corr integer,
    cl_bxm_code text,
    cl_branch_id integer,
    cl_local_code text,
    co_bxm_code text,
    co_branch_id integer,
    co_local_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.leads OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.leads TO gpadmin;




 insert into ods__fido.leads
 select * from leads_old;
 analyze ods__fido.leads;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_card
 ALTER TABLE ods__fido.ln_card RENAME TO ln_card_old;
 




CREATE TABLE ods__fido.ln_card (
    loan_id bigint,
    filial_code text,
    local_code text,
    condition smallint,
    card_type smallint,
    loan_type text,
    client_code text,
    claim_number text,
    loan_number smallint,
    loan_line_num text,
    committee_number text,
    date_committee timestamp with time zone,
    contract_code text,
    contract_date timestamp with time zone,
    contract_desc text,
    agr_num_notarial text,
    agr_date_notarial timestamp without time zone,
    doc_notarial_num text,
    doc_notarial_date timestamp with time zone,
    doc_gover_num text,
    doc_gover_date timestamp with time zone,
    open_date timestamp with time zone,
    close_date timestamp with time zone,
    currency text,
    summ_loan numeric,
    days_in_year smallint,
    grace_period smallint,
    fc_summ numeric,
    fc_desc text,
    form_delivery text,
    form_redemption text,
    term_loan_type text,
    eco_sec text,
    purpose_loan text,
    object_loan text,
    guar_class text,
    source_cred text,
    class_cred text,
    class_quality text,
    motive_revising text,
    date_revising timestamp with time zone,
    date_modify timestamp with time zone,
    err_message text,
    emp_code integer,
    sign_delivery text,
    manager_name text,
    sign_ebrd bit(1),
    gov_num text,
    gov_date timestamp with time zone,
    locking bit(1),
    day_redemp smallint,
    loanmonth numeric,
    percent_rate numeric,
    red_debt_month integer,
    red_perc_month integer,
    summinitial numeric,
    founders text,
    urgency_type text,
    claim_id bigint,
    client_id bigint,
    product_id integer,
    oked text,
    client_name text,
    client_uid numeric,
    created_by numeric,
    created_on timestamp with time zone,
    branch_id integer,
    lending_type text,
    purpose_lending text,
    cross_loan_id bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_card OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_card TO gpadmin;




 insert into ods__fido.ln_card
 select * from ln_card_old;
 analyze ods__fido.ln_card;
end;
$$



DO
$$
begin
 -- Таблица без уникального ключа ods__fido.ln_card_his уникальное поле:MD5(CONCAT(COALESCE("loan_id", 0), COALESCE("date_modify", '1970-01-01 00:00:00')))::uuid строк:1010936
 ALTER TABLE ods__fido.ln_card_his RENAME TO ln_card_his_old;
 




CREATE TABLE ods__fido.ln_card_his (
    loan_id integer,
    filial_code text,
    local_code text,
    condition smallint,
    card_type smallint,
    loan_type text,
    client_code text,
    claim_number text,
    loan_number integer,
    loan_line_num text,
    committee_number text,
    date_committee date,
    contract_code text,
    contract_date date,
    contract_desc text,
    agr_num_notarial text,
    agr_date_notarial date,
    doc_notarial_num text,
    doc_notarial_date date,
    doc_gover_num text,
    doc_gover_date date,
    open_date date,
    close_date date,
    currency text,
    summ_loan numeric,
    days_in_year smallint,
    grace_period integer,
    fc_summ numeric,
    fc_desc text,
    form_delivery text,
    form_redemption text,
    term_loan_type text,
    eco_sec text,
    purpose_loan text,
    object_loan text,
    guar_class text,
    source_cred text,
    class_cred text,
    class_quality text,
    motive_revising text,
    date_revising date,
    date_modify timestamp with time zone,
    err_message text,
    emp_code numeric,
    sign_delivery text,
    manager_name text,
    sign_ebrd bit(1,MD5(CONCAT(COALESCE("loan_id", 0), COALESCE("date_modify", '1970-01-01 00:00:00')))::uuid)  
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1),
    gov_num text,
    gov_date date,
    locking numeric,
    day_redemp numeric,
    loanmonth numeric,
    percent_rate numeric,
    red_debt_month numeric,
    red_perc_month numeric,
    summinitial numeric,
    founders text,
    urgency_type text,
    claim_id numeric,
    client_id numeric,
    product_id numeric,
    oked text,
    client_name text,
    client_uid numeric,
    created_by numeric,
    created_on date,
    branch_id numeric,
    lending_type text,
    purpose_lending text,
    cross_loan_id numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
,MD5(CONCAT(COALESCE("loan_id", 0), COALESCE("date_modify", '1970-01-01 00:00:00')))::uuid)  
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1)
 DISTRIBUTED (record_hash);


ALTER TABLE ods__fido.ln_card_his OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_card_his TO gpadmin;




 insert into ods__fido.ln_card_his
 select * from ln_card_his_old;
 analyze ods__fido.ln_card_his;
end;
$$



DO
$$
begin
 -- Таблица без уникального ключа ods__fido.ln_graph_debt уникальное поле:MD5(CONCAT(COALESCE("id", 0), COALESCE("date_red", '1970-01-01 00:00:00')))::uuid строк:2987061
 ALTER TABLE ods__fido.ln_graph_debt RENAME TO ln_graph_debt_old;
 




CREATE TABLE ods__fido.ln_graph_debt (
    id bigint,
    loan_id bigint,
    obligate_number text,
    date_red timestamp with time zone,
    summ_red numeric,
    sign_long smallint,
    condition smallint,
    emp_code integer,
    date_modify timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
,MD5(CONCAT(COALESCE("id", 0), COALESCE("date_red", '1970-01-01 00:00:00')))::uuid)  
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1)
 DISTRIBUTED (record_hash);


ALTER TABLE ods__fido.ln_graph_debt OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_graph_debt TO gpadmin;




 insert into ods__fido.ln_graph_debt
 select * from ln_graph_debt_old;
 analyze ods__fido.ln_graph_debt;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_interest_rates
 ALTER TABLE ods__fido.ln_interest_rates RENAME TO ln_interest_rates_old;
 




CREATE TABLE ods__fido.ln_interest_rates (
    code text,
    date_active timestamp with time zone,
    date_deactive timestamp with time zone,
    rate numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_interest_rates OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_interest_rates TO gpadmin;




 insert into ods__fido.ln_interest_rates
 select * from ln_interest_rates_old;
 analyze ods__fido.ln_interest_rates;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_loan_class_quality
 ALTER TABLE ods__fido.ln_loan_class_quality RENAME TO ln_loan_class_quality_old;
 




CREATE TABLE ods__fido.ln_loan_class_quality (
    filial_code text,
    loan_id bigint,
    class_quality text,
    oper_day date,
    next_date date,
    branch_id integer,
    local_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_loan_class_quality OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_loan_class_quality TO gpadmin;




 insert into ods__fido.ln_loan_class_quality
 select * from ln_loan_class_quality_old;
 analyze ods__fido.ln_loan_class_quality;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_loan_guar
 ALTER TABLE ods__fido.ln_loan_guar RENAME TO ln_loan_guar_old;
 




CREATE TABLE ods__fido.ln_loan_guar (
    loan_id bigint,
    guar_type text,
    sum_guar bigint,
    currency text,
    guar_id bigint,
    debtor_id integer,
    record_id integer,
    object_id integer,
    provision_group integer,
    credit_subject_type integer,
    collateral_type text,
    condition integer,
    begin_date date,
    end_date date,
    created_by integer,
    created_on timestamp with time zone,
    modified_by integer,
    modified_on timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_loan_guar OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_loan_guar TO gpadmin;




 insert into ods__fido.ln_loan_guar
 select * from ln_loan_guar_old;
 analyze ods__fido.ln_loan_guar;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_loan_guar_desc_dp
 ALTER TABLE ods__fido.ln_loan_guar_desc_dp RENAME TO ln_loan_guar_desc_dp_old;
 




CREATE TABLE ods__fido.ln_loan_guar_desc_dp (
    loan_id bigint,
    guar_type text,
    sign_depozit text,
    agr_number text,
    agr_date date,
    notarial_number text,
    notarial_date date,
    date_end date,
    code_bank text,
    name_depozit text,
    owner_depozit text,
    date_modify timestamp with time zone,
    emp_code integer,
    account text,
    guar_inn text,
    is_resident text,
    jfi text,
    guar_id bigint,
    created_by integer,
    created_on timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_loan_guar_desc_dp OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_loan_guar_desc_dp TO gpadmin;




 insert into ods__fido.ln_loan_guar_desc_dp
 select * from ln_loan_guar_desc_dp_old;
 analyze ods__fido.ln_loan_guar_desc_dp;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_loan_guar_desc_g
 ALTER TABLE ods__fido.ln_loan_guar_desc_g RENAME TO ln_loan_guar_desc_g_old;
 




CREATE TABLE ods__fido.ln_loan_guar_desc_g (
    loan_id bigint,
    guar_type text,
    agr_number text,
    agr_date date,
    guar_inn text,
    guar_mfo text,
    guar_acc text,
    red_date date,
    guar_name text,
    date_modify timestamp with time zone,
    emp_code integer,
    sign_confirm text,
    is_resident text,
    jfi text,
    guar_id bigint,
    organ_directive text,
    eco_sector text,
    client_type text,
    oked text,
    created_by integer,
    created_on timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_loan_guar_desc_g OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_loan_guar_desc_g TO gpadmin;




 insert into ods__fido.ln_loan_guar_desc_g
 select * from ln_loan_guar_desc_g_old;
 analyze ods__fido.ln_loan_guar_desc_g;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_loan_guar_desc_m
 ALTER TABLE ods__fido.ln_loan_guar_desc_m RENAME TO ln_loan_guar_desc_m_old;
 




CREATE TABLE ods__fido.ln_loan_guar_desc_m (
    loan_id bigint,
    guar_type text,
    owner_inn text,
    agr_number text,
    agr_date date,
    notarial_number text,
    notarial_date date,
    guar_name text,
    owner_desc text,
    date_modify timestamp with time zone,
    emp_code integer,
    sign_confirm text,
    is_resident text,
    jfi text,
    guar_id bigint,
    created_by integer,
    created_on timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_loan_guar_desc_m OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_loan_guar_desc_m TO gpadmin;




 insert into ods__fido.ln_loan_guar_desc_m
 select * from ln_loan_guar_desc_m_old;
 analyze ods__fido.ln_loan_guar_desc_m;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_loan_guar_desc_sp
 ALTER TABLE ods__fido.ln_loan_guar_desc_sp RENAME TO ln_loan_guar_desc_sp_old;
 




CREATE TABLE ods__fido.ln_loan_guar_desc_sp (
    loan_id bigint,
    guar_type text,
    agr_number text,
    agr_date date,
    notarial_number text,
    notarial_date date,
    date_end date,
    insurance_name text,
    date_modify timestamp with time zone,
    emp_code integer,
    guar_id bigint,
    insurance_inn text,
    dogovor_end_date date,
    ins_policy_beg_date date,
    prem_summ numeric,
    insurance_acc text,
    insurance_mfo text,
    created_by integer,
    created_on timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_loan_guar_desc_sp OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_loan_guar_desc_sp TO gpadmin;




 insert into ods__fido.ln_loan_guar_desc_sp
 select * from ln_loan_guar_desc_sp_old;
 analyze ods__fido.ln_loan_guar_desc_sp;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_percent_rate
 ALTER TABLE ods__fido.ln_percent_rate RENAME TO ln_percent_rate_old;
 




CREATE TABLE ods__fido.ln_percent_rate (
    loan_id bigint,
    perc_code_desc text,
    perc_type text,
    first_date date,
    perc_rate numeric,
    summa bigint,
    description text,
    emp_code integer,
    date_modify timestamp with time zone,
    share_combined_perc numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_percent_rate OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_percent_rate TO gpadmin;




 insert into ods__fido.ln_percent_rate
 select * from ln_percent_rate_old;
 analyze ods__fido.ln_percent_rate;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_s_perc_rate_type
 ALTER TABLE ods__fido.ln_s_perc_rate_type RENAME TO ln_s_perc_rate_type_old;
 




CREATE TABLE ods__fido.ln_s_perc_rate_type (
    per_rate_id text,
    per_rate_name text,
    order_by smallint,
    per_rate_ref text,
    is_mode text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_s_perc_rate_type OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_s_perc_rate_type TO gpadmin;




 insert into ods__fido.ln_s_perc_rate_type
 select * from ln_s_perc_rate_type_old;
 analyze ods__fido.ln_s_perc_rate_type;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_v_credit_guarant
 ALTER TABLE ods__fido.ln_v_credit_guarant RENAME TO ln_v_credit_guarant_old;
 




CREATE TABLE ods__fido.ln_v_credit_guarant (
    code text,
    group_code text,
    name text,
    date_activ date,
    date_deact date,
    date_correct timestamp with time zone,
    correcture text,
    condition text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_v_credit_guarant OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_v_credit_guarant TO gpadmin;




 insert into ods__fido.ln_v_credit_guarant
 select * from ln_v_credit_guarant_old;
 analyze ods__fido.ln_v_credit_guarant;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_v_credit_source
 ALTER TABLE ods__fido.ln_v_credit_source RENAME TO ln_v_credit_source_old;
 




CREATE TABLE ods__fido.ln_v_credit_source (
    code text,
    group_code text,
    name text,
    date_activ date,
    date_deact date,
    condition text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_v_credit_source OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_v_credit_source TO gpadmin;




 insert into ods__fido.ln_v_credit_source
 select * from ln_v_credit_source_old;
 analyze ods__fido.ln_v_credit_source;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_v_credit_type
 ALTER TABLE ods__fido.ln_v_credit_type RENAME TO ln_v_credit_type_old;
 




CREATE TABLE ods__fido.ln_v_credit_type (
    code text,
    group_code text,
    name text,
    date_activ date,
    date_deact date,
    date_correct timestamp with time zone,
    correcture text,
    condition text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_v_credit_type OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_v_credit_type TO gpadmin;




 insert into ods__fido.ln_v_credit_type
 select * from ln_v_credit_type_old;
 analyze ods__fido.ln_v_credit_type;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ln_v_purpose_cipher
 ALTER TABLE ods__fido.ln_v_purpose_cipher RENAME TO ln_v_purpose_cipher_old;
 




CREATE TABLE ods__fido.ln_v_purpose_cipher (
    code text,
    group_code text,
    sub_code text,
    code1 text,
    name text,
    purpose_codes text,
    date_activ date,
    date_deact date,
    date_correct timestamp with time zone,
    correcture text,
    condition text,
    condition_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ln_v_purpose_cipher OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ln_v_purpose_cipher TO gpadmin;




 insert into ods__fido.ln_v_purpose_cipher
 select * from ln_v_purpose_cipher_old;
 analyze ods__fido.ln_v_purpose_cipher;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.ref_type_client
 ALTER TABLE ods__fido.ref_type_client RENAME TO ref_type_client_old;
 




CREATE TABLE ods__fido.ref_type_client (
    code text,
    code_char text,
    name text,
    date_activ date,
    date_deact date,
    condition text,
    ref_uid bigint,
    date_apply timestamp with time zone,
    date_cancel timestamp with time zone,
    version_id integer,
    modified_on timestamp with time zone,
    name_uz text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.ref_type_client OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.ref_type_client TO gpadmin;




 insert into ods__fido.ref_type_client
 select * from ref_type_client_old;
 analyze ods__fido.ref_type_client;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.rep_spr_depon
 ALTER TABLE ods__fido.rep_spr_depon RENAME TO rep_spr_depon_old;
 




CREATE TABLE ods__fido.rep_spr_depon (
    code_rep text,
    code_line smallint,
    name_line text,
    formula text,
    perc_nac numeric,
    perc_val numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.rep_spr_depon OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.rep_spr_depon TO gpadmin;




 insert into ods__fido.rep_spr_depon
 select * from rep_spr_depon_old;
 analyze ods__fido.rep_spr_depon;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.rep_spr_depon_desc
 ALTER TABLE ods__fido.rep_spr_depon_desc RENAME TO rep_spr_depon_desc_old;
 




CREATE TABLE ods__fido.rep_spr_depon_desc (
    code_rep text,
    code_line smallint,
    code_coa text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.rep_spr_depon_desc OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.rep_spr_depon_desc TO gpadmin;




 insert into ods__fido.rep_spr_depon_desc
 select * from rep_spr_depon_desc_old;
 analyze ods__fido.rep_spr_depon_desc;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.saldo_y
 ALTER TABLE ods__fido.saldo_y RENAME TO saldo_y_old;
 




CREATE TABLE ods__fido.saldo_y (
    id integer,
    oper_day date,
    account_code text,
    code_filial text,
    balance_out text,
    saldo_in bigint,
    saldo_out bigint,
    saldo_equival_in bigint,
    saldo_equival_out bigint,
    turnover_debit bigint,
    turnover_credit bigint,
    turnover_all_debit bigint,
    turnover_all_credit bigint,
    lead_last_date date,
    sb_code text,
    branch_id integer,
    local_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.saldo_y OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.saldo_y TO gpadmin;




 insert into ods__fido.saldo_y
 select * from saldo_y_old;
 analyze ods__fido.saldo_y;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.sl_emp_salarys
 ALTER TABLE ods__fido.sl_emp_salarys RENAME TO sl_emp_salarys_old;
 




CREATE TABLE ods__fido.sl_emp_salarys (
    line_id integer,
    filial text,
    emp_id bigint,
    tab_num text,
    salary_calculated numeric,
    salary_approved numeric,
    approved_flag text,
    salary_begin timestamp without time zone,
    salary_end timestamp without time zone,
    state text,
    staffing_id integer,
    salary_id bigint,
    created_by bigint,
    creation_date timestamp without time zone,
    local_code text,
    branch_id integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.sl_emp_salarys OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.sl_emp_salarys TO gpadmin;




 insert into ods__fido.sl_emp_salarys
 select * from sl_emp_salarys_old;
 analyze ods__fido.sl_emp_salarys;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.sl_h_calcs
 ALTER TABLE ods__fido.sl_h_calcs RENAME TO sl_h_calcs_old;
 




CREATE TABLE ods__fido.sl_h_calcs (
    calc_id bigint,
    filial bigint,
    emp_id bigint,
    tab_num bigint,
    pay_id integer,
    pay_name text,
    period date,
    summ numeric,
    state text,
    created_by text,
    creation_date timestamp with time zone,
    claim_id text,
    cont_id text,
    paid numeric,
    cs_id text,
    refresh text,
    summ_alt numeric,
    sync_id text,
    is_loaded text,
    local_code text,
    branch_id text,
    percent text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.sl_h_calcs OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.sl_h_calcs TO gpadmin;




 insert into ods__fido.sl_h_calcs
 select * from sl_h_calcs_old;
 analyze ods__fido.sl_h_calcs;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.transacts
 ALTER TABLE ods__fido.transacts RENAME TO transacts_old;
 




CREATE TABLE ods__fido.transacts (
    lead_id bigint,
    code_filial text,
    op_dc smallint,
    acc_id bigint,
    state_id smallint,
    curr_day date,
    deal_id bigint,
    document_id bigint,
    order_num smallint,
    operation_id bigint,
    client_id bigint,
    product_id integer,
    branch_id integer,
    local_code text,
    calendar_day date,
    bxm_code text,
    amount numeric,
    eqv_amount numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.transacts OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.transacts TO gpadmin;




 insert into ods__fido.transacts
 select * from transacts_old;
 analyze ods__fido.transacts;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.v_bank
 ALTER TABLE ods__fido.v_bank RENAME TO v_bank_old;
 




CREATE TABLE ods__fido.v_bank (
    code text,
    bank_type_code text,
    region_code text,
    header_code text,
    union_code text,
    tcr_code text,
    rkc_code text,
    name text,
    adress text,
    status_code text,
    account_type text,
    date_open date,
    date_close date,
    active text,
    email text,
    server_alias text,
    connect_type text,
    allow_currency text,
    date_activ date,
    date_deact date,
    date_correct timestamp with time zone,
    correcture text,
    district_code text,
    num_change_office smallint,
    condition text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.v_bank OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.v_bank TO gpadmin;




 insert into ods__fido.v_bank
 select * from v_bank_old;
 analyze ods__fido.v_bank;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.v_coa
 ALTER TABLE ods__fido.v_coa RENAME TO v_coa_old;
 




CREATE TABLE ods__fido.v_coa (
    destination text,
    code text,
    name text,
    type text,
    section_code text,
    type_acc_code text,
    reverse_code text,
    client_code text,
    gr_risk_code text,
    date_activ date,
    date_deact date,
    date_correct timestamp with time zone,
    correcture text,
    sign_nibbd smallint,
    condition text,
    row_id text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.v_coa OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.v_coa TO gpadmin;




 insert into ods__fido.v_coa
 select * from v_coa_old;
 analyze ods__fido.v_coa;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.v_currency
 ALTER TABLE ods__fido.v_currency RENAME TO v_currency_old;
 




CREATE TABLE ods__fido.v_currency (
    code text,
    char_code text,
    name text,
    scale smallint,
    scale_name text,
    hard text,
    allow text,
    date_activ date,
    date_deact date,
    date_correct timestamp without time zone,
    correcture text,
    condition text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.v_currency OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.v_currency TO gpadmin;




 insert into ods__fido.v_currency
 select * from v_currency_old;
 analyze ods__fido.v_currency;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__fido.v_region
 ALTER TABLE ods__fido.v_region RENAME TO v_region_old;
 




CREATE TABLE ods__fido.v_region (
    code text,
    name text,
    order_rep smallint,
    date_activ date,
    date_deact date,
    date_correct timestamp with time zone,
    correcture text,
    condition text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__fido.v_region OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido.v_region TO gpadmin;




 insert into ods__fido.v_region
 select * from v_region_old;
 analyze ods__fido.v_region;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.cash_unit_statuses
 ALTER TABLE ods__ktc.cash_unit_statuses RENAME TO cash_unit_statuses_old;
 




CREATE TABLE ods__ktc.cash_unit_statuses (
    cash_unit_status_id smallint,
    status_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.cash_unit_statuses OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.cash_unit_statuses TO gpadmin;




 insert into ods__ktc.cash_unit_statuses
 select * from cash_unit_statuses_old;
 analyze ods__ktc.cash_unit_statuses;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.cash_unit_types
 ALTER TABLE ods__ktc.cash_unit_types RENAME TO cash_unit_types_old;
 




CREATE TABLE ods__ktc.cash_unit_types (
    cash_unit_type_id smallint,
    type_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.cash_unit_types OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.cash_unit_types TO gpadmin;




 insert into ods__ktc.cash_unit_types
 select * from cash_unit_types_old;
 analyze ods__ktc.cash_unit_types;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.clients
 ALTER TABLE ods__ktc.clients RENAME TO clients_old;
 




CREATE TABLE ods__ktc.clients (
    client_id integer,
    ktc_guid text,
    client_name text,
    latitude real,
    longitude real,
    timezone text,
    client_type smallint,
    active text,
    level1_region_id smallint,
    level2_region_id smallint,
    level3_region_id smallint,
    level4_region_id smallint,
    level5_region_id smallint,
    deleted_timestamp timestamp without time zone,
    hypervisor_active text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.clients OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.clients TO gpadmin;




 insert into ods__ktc.clients
 select * from clients_old;
 analyze ods__ktc.clients;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.component_list
 ALTER TABLE ods__ktc.component_list RENAME TO component_list_old;
 




CREATE TABLE ods__ktc.component_list (
    component_id smallint,
    component_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.component_list OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.component_list TO gpadmin;




 insert into ods__ktc.component_list
 select * from component_list_old;
 analyze ods__ktc.component_list;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.currencies
 ALTER TABLE ods__ktc.currencies RENAME TO currencies_old;
 




CREATE TABLE ods__ktc.currencies (
    currency_id smallint,
    currency text,
    currency_description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.currencies OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.currencies TO gpadmin;




 insert into ods__ktc.currencies
 select * from currencies_old;
 analyze ods__ktc.currencies;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.denominations
 ALTER TABLE ods__ktc.denominations RENAME TO denominations_old;
 




CREATE TABLE ods__ktc.denominations (
    denomination_id smallint,
    currency_id smallint,
    currency_value numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.denominations OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.denominations TO gpadmin;




 insert into ods__ktc.denominations
 select * from denominations_old;
 analyze ods__ktc.denominations;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.historical_cash_unit_status_p
 ALTER TABLE ods__ktc.historical_cash_unit_status_p RENAME TO historical_cash_unit_status_p_old;
 




CREATE TABLE ods__ktc.historical_cash_unit_status_p (
    client_id integer,
    component_id smallint,
    cash_unit smallint,
    type_id smallint,
    status_id smallint,
    timestmp timestamp without time zone,
    currency_id smallint,
    currency_value numeric,
    unit_count integer,
    total_value numeric,
    added_time timestamp without time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.historical_cash_unit_status_p OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.historical_cash_unit_status_p TO gpadmin;




 insert into ods__ktc.historical_cash_unit_status_p
 select * from historical_cash_unit_status_p_old;
 analyze ods__ktc.historical_cash_unit_status_p;
end;
$$



DO
$$
begin
 -- Таблица без уникального ключа ods__ktc.historical_counters_p уникальное поле:MD5(CONCAT(COALESCE("added_time", '1970-01-01 00:00:00'), COALESCE("timestmp", '1970-01-01 00:00:00'), COALESCE("denomination_id", 0), COALESCE("property_id", 0), COALESCE("component_id", 0)))::uuid строк:1348770
 ALTER TABLE ods__ktc.historical_counters_p RENAME TO historical_counters_p_old;
 




CREATE TABLE ods__ktc.historical_counters_p (
    client_id integer,
    component_id smallint,
    property_id smallint,
    denomination_id smallint,
    timestmp timestamp without time zone,
    counter_value integer,
    counter_delta integer,
    counter_reset boolean,
    until_timestmp timestamp without time zone,
    added_time timestamp without time zone,
    updated_time timestamp without time zone,
    transaction_id bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
,MD5(CONCAT(COALESCE("added_time", '1970-01-01 00:00:00'), COALESCE("timestmp", '1970-01-01 00:00:00'), COALESCE("denomination_id", 0), COALESCE("property_id", 0), COALESCE("component_id", 0)))::uuid)  
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1)
 DISTRIBUTED (record_hash);


ALTER TABLE ods__ktc.historical_counters_p OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.historical_counters_p TO gpadmin;




 insert into ods__ktc.historical_counters_p
 select * from historical_counters_p_old;
 analyze ods__ktc.historical_counters_p;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.overall_availability_p
 ALTER TABLE ods__ktc.overall_availability_p RENAME TO overall_availability_p_old;
 




CREATE TABLE ods__ktc.overall_availability_p (
    overall_avail_id bigint,
    client_id integer,
    timestmp timestamp without time zone,
    timestmp_local timestamp without time zone,
    sec_available smallint,
    sec_unavailable smallint,
    sec_is smallint,
    sec_oos smallint,
    sec_rs smallint,
    sec_ls smallint,
    sec_sus smallint,
    sec_fb smallint,
    sec_anr smallint,
    sec_knr smallint,
    dis_reason_id smallint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.overall_availability_p OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.overall_availability_p TO gpadmin;




 insert into ods__ktc.overall_availability_p
 select * from overall_availability_p_old;
 analyze ods__ktc.overall_availability_p;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.overall_unavailable_reasons_p
 ALTER TABLE ods__ktc.overall_unavailable_reasons_p RENAME TO overall_unavailable_reasons_p_old;
 




CREATE TABLE ods__ktc.overall_unavailable_reasons_p (
    overall_avail_id bigint,
    una_reason_id smallint,
    sec_duration smallint,
    service_state smallint,
    timestmp timestamp without time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.overall_unavailable_reasons_p OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.overall_unavailable_reasons_p TO gpadmin;




 insert into ods__ktc.overall_unavailable_reasons_p
 select * from overall_unavailable_reasons_p_old;
 analyze ods__ktc.overall_unavailable_reasons_p;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.property_list
 ALTER TABLE ods__ktc.property_list RENAME TO property_list_old;
 




CREATE TABLE ods__ktc.property_list (
    property_id smallint,
    property_name text,
    category text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.property_list OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.property_list TO gpadmin;




 insert into ods__ktc.property_list
 select * from property_list_old;
 analyze ods__ktc.property_list;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.stx_field_lookups
 ALTER TABLE ods__ktc.stx_field_lookups RENAME TO stx_field_lookups_old;
 




CREATE TABLE ods__ktc.stx_field_lookups (
    field_lookup_id integer,
    field_id smallint,
    disambiguation text,
    field_code text,
    field_description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.stx_field_lookups OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.stx_field_lookups TO gpadmin;




 insert into ods__ktc.stx_field_lookups
 select * from stx_field_lookups_old;
 analyze ods__ktc.stx_field_lookups;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.transaction_data_p
 ALTER TABLE ods__ktc.transaction_data_p RENAME TO transaction_data_p_old;
 




CREATE TABLE ods__ktc.transaction_data_p (
    transaction_id bigint,
    transaction_uuid uuid,
    session_id bigint,
    transaction_timestamp timestamp without time zone,
    transaction_timestamp_local timestamp without time zone,
    start_client_ej_id bigint,
    end_client_ej_id bigint,
    client_id integer,
    tx_type_field_lookup_id integer,
    amount numeric,
    completion_field_lookup_id integer,
    reason_field_lookup_id integer,
    effective_amount numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.transaction_data_p OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.transaction_data_p TO gpadmin;




 insert into ods__ktc.transaction_data_p
 select * from transaction_data_p_old;
 analyze ods__ktc.transaction_data_p;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.unavailable_reasons
 ALTER TABLE ods__ktc.unavailable_reasons RENAME TO unavailable_reasons_old;
 




CREATE TABLE ods__ktc.unavailable_reasons (
    una_reason_id smallint,
    una_reason_message text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.unavailable_reasons OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.unavailable_reasons TO gpadmin;




 insert into ods__ktc.unavailable_reasons
 select * from unavailable_reasons_old;
 analyze ods__ktc.unavailable_reasons;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.accounting_register
 ALTER TABLE ods__odata_1c.accounting_register RENAME TO accounting_register_old;
 




CREATE TABLE ods__odata_1c.accounting_register (
    recorder uuid,
    recorder_type text,
    period timestamp without time zone,
    line_number smallint,
    active boolean,
    account_dr_key uuid,
    account_cr_key uuid,
    organization_key uuid,
    currency_dr_key uuid,
    currency_cr_key uuid,
    department_dr_key uuid,
    department_cr_key uuid,
    amount numeric,
    currency_amount_dr numeric,
    currency_amount_cr numeric,
    quantity_dr numeric,
    quantity_cr numeric,
    fixed_assets_amount_dr numeric,
    fixed_assets_amount_cr numeric,
    pr_amount_dr numeric,
    pr_amount_cr numeric,
    vr_amount_dr numeric,
    vr_amount_cr numeric,
    content text,
    do_not_adjust_cost boolean,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.accounting_register OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.accounting_register TO gpadmin;




 insert into ods__odata_1c.accounting_register
 select * from accounting_register_old;
 analyze ods__odata_1c.accounting_register;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.accounts
 ALTER TABLE ods__odata_1c.accounts RENAME TO accounts_old;
 




CREATE TABLE ods__odata_1c.accounts (
    ref_key uuid,
    parent_key uuid,
    code text,
    description text,
    type text,
    off_balance boolean,
    code_orig text,
    actual boolean,
    manual_sub_conto boolean,
    foreign_currency boolean,
    quantitive boolean,
    by_departments boolean,
    tax boolean,
    predefined boolean,
    predefined_data_name text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.accounts OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.accounts TO gpadmin;




 insert into ods__odata_1c.accounts
 select * from accounts_old;
 analyze ods__odata_1c.accounts;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.balance_and_turnovers
 ALTER TABLE ods__odata_1c.balance_and_turnovers RENAME TO balance_and_turnovers_old;
 




CREATE TABLE ods__odata_1c.balance_and_turnovers (
    account_key uuid,
    ext_dimension_1 text,
    ext_dimension_1_type text,
    ext_dimension_2 text,
    ext_dimension_2_type text,
    ext_dimension_3 text,
    ext_dimension_3_type text,
    fc_key uuid,
    department_key uuid,
    sum_opening_balance numeric,
    sum_opening_balance_dr numeric,
    sum_opening_balance_cr numeric,
    sum_turnover numeric,
    sum_turnover_dr numeric,
    sum_turnover_cr numeric,
    sum_closing_balance numeric,
    sum_closing_balance_dr numeric,
    sum_closing_balance_cr numeric,
    sum_closing_splitted_balance_dr numeric,
    sum_closing_splitted_balance_cr numeric,
    fc_sum_opening_balance numeric,
    fc_sum_opening_balance_dr numeric,
    fc_sum_opening_balance_cr numeric,
    fc_sum_opening_splitted_balance_dr numeric,
    fc_sum_opening_splitted_balance_cr numeric,
    fc_sum_turnover numeric,
    fc_sum_turnover_dr numeric,
    fc_sum_turnover_cr numeric,
    fc_sum_closing_balance numeric,
    fc_sum_closing_balance_dr numeric,
    fc_sum_closing_balance_cr numeric,
    fc_sum_closing_splitted_balance_dr numeric,
    fc_sum_closing_splitted_balance_cr numeric,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.balance_and_turnovers OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.balance_and_turnovers TO gpadmin;




 insert into ods__odata_1c.balance_and_turnovers
 select * from balance_and_turnovers_old;
 analyze ods__odata_1c.balance_and_turnovers;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.calculation_types
 ALTER TABLE ods__odata_1c.calculation_types RENAME TO calculation_types_old;
 




CREATE TABLE ods__odata_1c.calculation_types (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    code text,
    description text,
    action_period_is_basic boolean,
    category_calculation_unpaid_time text,
    additional_ordering_feature smallint,
    type_of_calculation_for_nu text,
    income_code_ndfl_key uuid,
    designation_in_timekeeping_table_key uuid,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.calculation_types OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.calculation_types TO gpadmin;




 insert into ods__odata_1c.calculation_types
 select * from calculation_types_old;
 analyze ods__odata_1c.calculation_types;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.cfo
 ALTER TABLE ods__odata_1c.cfo RENAME TO cfo_old;
 




CREATE TABLE ods__odata_1c.cfo (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    code text,
    description text,
    predefined boolean,
    predefined_data_name text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.cfo OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.cfo TO gpadmin;




 insert into ods__odata_1c.cfo
 select * from cfo_old;
 analyze ods__odata_1c.cfo;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.construction_object
 ALTER TABLE ods__odata_1c.construction_object RENAME TO construction_object_old;
 




CREATE TABLE ods__odata_1c.construction_object (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    number text,
    date timestamp without time zone,
    posted boolean,
    operation_type text,
    organization_key uuid,
    asset_location_key uuid,
    asset_event_key uuid,
    nomenclature_key uuid,
    warehouse_key uuid,
    income_article_key uuid,
    organization_department_key uuid,
    construction_object_key uuid,
    fixed_asset_account_key uuid,
    bu_cost numeric,
    nu_cost numeric,
    pr_cost numeric,
    vr_cost numeric,
    bu_cost_amortization_method text,
    bu_responsible_employee_key uuid,
    receipt_method text,
    accounting_account_key uuid,
    amortization_account_key uuid,
    bu_amortization boolean,
    bu_amortization_from_date_of_exploitation boolean,
    bu_amortization_method text,
    amortization_expense_reflection_methods_key uuid,
    bu_useful_life smallint,
    bu_amortization_schedule_key uuid,
    bu_annual_amortization_rate numeric,
    bu_acceleration_coefficient numeric,
    bu_production_parameter_key uuid,
    bu_expected_production_volume numeric,
    nu_cost_inclusion_method text,
    initial_cost_specified boolean,
    nu_initial_cost numeric,
    manual_correction boolean,
    responsible_employee_key uuid,
    comment text,
    nu_empty_row text,
    counterparty_key uuid,
    counterparty_contract_key uuid,
    lease_payments_reflection_method_key uuid,
    liquidation_cost numeric,
    acquisition_date date,
    usn_useful_life smallint,
    nu_amortization_coefficient numeric,
    nu_special_coefficient numeric,
    nu_capital_investment_percentage numeric,
    premium_amortization_expense_account_key uuid,
    premium_amortization_expense_subaccount_1 text,
    premium_amortization_expense_subaccount_1_type text,
    premium_amortization_expense_subaccount_2 text,
    premium_amortization_expense_subaccount_2_type text,
    premium_amortization_expense_subaccount_3 text,
    premium_amortization_expense_subaccount_3_type text,
    premium_amortization_expense_department_key uuid,
    cost_inclusion_expense_reflection_method_key uuid,
    other_expense_article_key uuid,
    usn_cost numeric,
    usn_cost_inclusion_method text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.construction_object OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.construction_object TO gpadmin;




 insert into ods__odata_1c.construction_object
 select * from construction_object_old;
 analyze ods__odata_1c.construction_object;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.counterparties
 ALTER TABLE ods__odata_1c.counterparties RENAME TO counterparties_old;
 




CREATE TABLE ods__odata_1c.counterparties (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    parent_key uuid,
    is_folder boolean,
    code text,
    description text,
    description_full text,
    is_group_dep boolean,
    counterparty_type text,
    country_key uuid,
    parent_counterparty_key uuid,
    inn text,
    kpp text,
    okpo_code text,
    proof_doc text,
    core_bank_acc_key uuid,
    delete_primary_aggr_key uuid,
    main_contact_key uuid,
    comment text,
    addinfo text,
    delete_counterparty text,
    proper_inn boolean,
    propen_kpp boolean,
    expand_inn text,
    expand_kpp text,
    tax_num text,
    registration_num text,
    gov_party boolean,
    gov_party_type text,
    gov_party_code text,
    doc_series_and_num text,
    doc_date date,
    date_registration date,
    okonx text,
    soogu text,
    okved2 text,
    name_okved2 text,
    reg_code_nds text,
    foreign_organization boolean,
    fl_key uuid,
    soato_code text,
    pinfl text,
    nds_discount text,
    nds_discount_2022 text,
    intradepartmental_org boolean,
    contact_info jsonb,
    add_req jsonb,
    kpp_hist jsonb,
    naming_hist jsonb,
    contact_info_hist jsonb,
    predefined boolean,
    predefined_data_name text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.counterparties OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.counterparties TO gpadmin;




 insert into ods__odata_1c.counterparties
 select * from counterparties_old;
 analyze ods__odata_1c.counterparties;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.currencies
 ALTER TABLE ods__odata_1c.currencies RENAME TO currencies_old;
 




CREATE TABLE ods__odata_1c.currencies (
    ref_key uuid,
    code integer,
    description text,
    currency_name text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.currencies OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.currencies TO gpadmin;




 insert into ods__odata_1c.currencies
 select * from currencies_old;
 analyze ods__odata_1c.currencies;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.departments
 ALTER TABLE ods__odata_1c.departments RENAME TO departments_old;
 




CREATE TABLE ods__odata_1c.departments (
    ref_key uuid,
    owner_key uuid,
    parent_key uuid,
    code text,
    description text,
    head_office_key uuid,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.departments OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.departments TO gpadmin;




 insert into ods__odata_1c.departments
 select * from departments_old;
 analyze ods__odata_1c.departments;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.employee_personal_data
 ALTER TABLE ods__odata_1c.employee_personal_data RENAME TO employee_personal_data_old;
 




CREATE TABLE ods__odata_1c.employee_personal_data (
    ref_key uuid,
    full_name text,
    birth_date date,
    sex text,
    pfr_number text,
    pinfl text,
    code text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.employee_personal_data OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.employee_personal_data TO gpadmin;




 insert into ods__odata_1c.employee_personal_data
 select * from employee_personal_data_old;
 analyze ods__odata_1c.employee_personal_data;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.employee_register
 ALTER TABLE ods__odata_1c.employee_register RENAME TO employee_register_old;
 




CREATE TABLE ods__odata_1c.employee_register (
    individual_key uuid,
    employee_key uuid,
    head_office_key uuid,
    office_key uuid,
    department_key uuid,
    position_key uuid,
    start_date date,
    end_date date,
    primary_employment boolean,
    employment_type text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.employee_register OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.employee_register TO gpadmin;




 insert into ods__odata_1c.employee_register
 select * from employee_register_old;
 analyze ods__odata_1c.employee_register;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.employees
 ALTER TABLE ods__odata_1c.employees RENAME TO employees_old;
 




CREATE TABLE ods__odata_1c.employees (
    ref_key uuid,
    code text,
    description text,
    individual_key uuid,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.employees OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.employees TO gpadmin;




 insert into ods__odata_1c.employees
 select * from employees_old;
 analyze ods__odata_1c.employees;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.employees_hist
 ALTER TABLE ods__odata_1c.employees_hist RENAME TO employees_hist_old;
 




CREATE TABLE ods__odata_1c.employees_hist (
    recorder uuid,
    recorder_type text,
    period date,
    line_number smallint,
    active boolean,
    employee_key uuid,
    head_office_key uuid,
    individual_key uuid,
    organization_key uuid,
    department_key uuid,
    position_key uuid,
    event_type text,
    employment_type text,
    primary_employee_key uuid,
    primary_employee boolean,
    rate numeric,
    agreement_type text,
    valid_to_date date,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.employees_hist OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.employees_hist TO gpadmin;




 insert into ods__odata_1c.employees_hist
 select * from employees_hist_old;
 analyze ods__odata_1c.employees_hist;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.expenses
 ALTER TABLE ods__odata_1c.expenses RENAME TO expenses_old;
 




CREATE TABLE ods__odata_1c.expenses (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    number text,
    date timestamp without time zone,
    posted boolean,
    operation_type text,
    tax_key uuid,
    tax_obligation_type text,
    organization_key uuid,
    org_account_key uuid,
    org_department_key uuid,
    bank_account_key uuid,
    incoming_doc_num text,
    incoming_doc_date timestamp without time zone,
    counterparty uuid,
    counterparty_type text,
    counterparty_acc_key uuid,
    document_sum numeric,
    counterparty_payment_rep_acc uuid,
    subconto_dt1 text,
    subconto_dt1_type text,
    subconto_dt2 text,
    subconto_dt2_type text,
    subconto_dt3 text,
    subconto_dt3_type text,
    department_dt_key uuid,
    odds_state_key uuid,
    del_fl_key uuid,
    payment_purpose text,
    responsible_key text,
    comment text,
    base_document text,
    base_document_type text,
    counterparty_aggr_key uuid,
    doc_currency_key uuid,
    tax_content text,
    graph4_tax numeric,
    graph5_tax numeric,
    graph6_tax numeric,
    graph7_tax numeric,
    envd_tax_income boolean,
    envd_tax_outcome boolean,
    vat_tax numeric,
    delete_manual_tax_corr boolean,
    manual_corr boolean,
    bank_not_admitted boolean,
    tax_period date,
    payment_statement uuid,
    payment_statement_type text,
    tax_org_reg_key uuid,
    tax_payer_key uuid,
    inps_acc_key uuid,
    inps_dobr_acc_key uuid,
    div_perc_payment_type text,
    div_sum numeric,
    rate_percent numeric,
    date_accrual date,
    date_payment numeric,
    counterparty_payment_acc uuid,
    tax_value numeric,
    statement_date date,
    payment_type_key uuid,
    statement_status text,
    salary_account_key uuid,
    exchange_rate_document numeric,
    document_multiplier smallint,
    accountables_acc_key uuid,
    change_not_accepted boolean,
    payoff_type text,
    cor_acc_key uuid,
    fine_sum numeric,
    exp_acc_key uuid,
    exp_states_key uuid,
    in_report boolean,
    payment_state_key uuid,
    cfr_key uuid,
    payment_description jsonb,
    salary_transfer jsonb,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.expenses OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.expenses TO gpadmin;




 insert into ods__odata_1c.expenses
 select * from expenses_old;
 analyze ods__odata_1c.expenses;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.expenses_dict
 ALTER TABLE ods__odata_1c.expenses_dict RENAME TO expenses_dict_old;
 




CREATE TABLE ods__odata_1c.expenses_dict (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    code text,
    description text,
    predefined boolean,
    predefined_data_name text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.expenses_dict OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.expenses_dict TO gpadmin;




 insert into ods__odata_1c.expenses_dict
 select * from expenses_dict_old;
 analyze ods__odata_1c.expenses_dict;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.extend_employee_regions
 ALTER TABLE ods__odata_1c.extend_employee_regions RENAME TO extend_employee_regions_old;
 




CREATE TABLE ods__odata_1c.extend_employee_regions (
    period date,
    employee_key character varying(36),
    region_key character varying(36),
    dwh_created_at timestamp with time zone,
    dwh_job_id text,
    record_hash uuid
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.extend_employee_regions OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.extend_employee_regions TO gpadmin;




 insert into ods__odata_1c.extend_employee_regions
 select * from extend_employee_regions_old;
 analyze ods__odata_1c.extend_employee_regions;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.extend_employee_regions_catalog
 ALTER TABLE ods__odata_1c.extend_employee_regions_catalog RENAME TO extend_employee_regions_catalog_old;
 




CREATE TABLE ods__odata_1c.extend_employee_regions_catalog (
    region_key character varying(36),
    parent_key character varying(36),
    code character varying(10),
    deletion_mark character varying(5),
    description character varying(256),
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.extend_employee_regions_catalog OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.extend_employee_regions_catalog TO gpadmin;




 insert into ods__odata_1c.extend_employee_regions_catalog
 select * from extend_employee_regions_catalog_old;
 analyze ods__odata_1c.extend_employee_regions_catalog;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.extend_employee_titles_catalog
 ALTER TABLE ods__odata_1c.extend_employee_titles_catalog RENAME TO extend_employee_titles_catalog_old;
 




CREATE TABLE ods__odata_1c.extend_employee_titles_catalog (
    titul_key character varying(36),
    code character varying(10),
    deletion_mark character varying(5),
    description character varying(256),
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.extend_employee_titles_catalog OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.extend_employee_titles_catalog TO gpadmin;




 insert into ods__odata_1c.extend_employee_titles_catalog
 select * from extend_employee_titles_catalog_old;
 analyze ods__odata_1c.extend_employee_titles_catalog;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.intangible_asset
 ALTER TABLE ods__odata_1c.intangible_asset RENAME TO intangible_asset_old;
 




CREATE TABLE ods__odata_1c.intangible_asset (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    number text,
    date timestamp without time zone,
    posted boolean,
    asset_type text,
    organization_key uuid,
    organization_department_key uuid,
    intangible_asset_key uuid,
    fixed_asset_account_key uuid,
    expense_reflection_method_key uuid,
    accounting_account_key uuid,
    bu_cost numeric,
    receipt_method text,
    bu_amortization boolean,
    bu_useful_life smallint,
    bu_amortization_method text,
    bu_coefficient numeric,
    amortization_calculation_volume numeric,
    amortization_account_key uuid,
    payment_document_details text,
    nu_cost numeric,
    pr_cost numeric,
    vr_cost numeric,
    niokr_expense_write_off_order_nu text,
    acquisition_date date,
    nu_amortization boolean,
    nu_useful_life smallint,
    nu_special_coefficient numeric,
    usn_cost numeric,
    usn_useful_life smallint,
    usn_cost_inclusion_order text,
    comment text,
    responsible_employee_key uuid,
    manual_correction boolean,
    vat_accounting_method text,
    nu_cost_inclusion_order text,
    cost_inclusion_expense_reflection_method_key uuid,
    other_expense_article_key uuid,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.intangible_asset OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.intangible_asset TO gpadmin;




 insert into ods__odata_1c.intangible_asset
 select * from intangible_asset_old;
 analyze ods__odata_1c.intangible_asset;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.invoice
 ALTER TABLE ods__odata_1c.invoice RENAME TO invoice_old;
 




CREATE TABLE ods__odata_1c.invoice (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    number text,
    date date,
    posted boolean,
    organization_key uuid,
    invoice_type text,
    counterparty_key uuid,
    counterparty_contract_key uuid,
    incoming_document_number text,
    incoming_document_date date,
    correction boolean,
    correction_number smallint,
    correction_date date,
    corrected_invoice_key uuid,
    source_document_number smallint,
    source_document_date date,
    delete_correction_from_source_document boolean,
    delete_correction_number_from_source_document integer,
    delete_correction_date_from_source_document date,
    vat_claimed boolean,
    seller_key uuid,
    principal_key uuid,
    principal_contract_key uuid,
    basis_document uuid,
    basis_document_type text,
    invoice_without_vat boolean,
    operation_code text,
    receipt_method_code smallint,
    document_amount numeric,
    increase_amount numeric,
    decrease_amount numeric,
    vat_amount numeric,
    vat_increase_amount numeric,
    vat_decrease_amount numeric,
    document_currency_key uuid,
    responsible_key uuid,
    comment text,
    manual_correction boolean,
    formed_with_initial_vat_balance boolean,
    delete_correction_invoice boolean,
    delete_on_advance boolean,
    strict_reporting_form boolean,
    counterparty_tin text,
    consolidated_correction boolean,
    number_presentation text,
    return_through_commissioner boolean,
    subcommissioner_key uuid,
    customer_invoice_key uuid,
    document_commission_amount numeric,
    vat_commission_amount numeric,
    commission_increase_amount numeric,
    commission_decrease_amount numeric,
    vat_commission_increase_amount numeric,
    vat_commission_decrease_amount numeric,
    consolidated_commission boolean,
    own_error_correction boolean,
    incoming_document_number_before_change text,
    incoming_document_date_before_change date,
    correction_number_before_change smallint,
    correction_date_before_change date,
    operation_code_before_change smallint,
    counterparty_tin_before_change text,
    counterparty_kpp_before_change text,
    counterparty_inn text,
    decrease_operation_code smallint,
    decrease_operation_code_before_change smallint,
    vat_deduction_period text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.invoice OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.invoice TO gpadmin;




 insert into ods__odata_1c.invoice
 select * from invoice_old;
 analyze ods__odata_1c.invoice;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.other_incomes_and_spending
 ALTER TABLE ods__odata_1c.other_incomes_and_spending RENAME TO other_incomes_and_spending_old;
 




CREATE TABLE ods__odata_1c.other_incomes_and_spending (
    ref_key uuid,
    deletion_mark boolean,
    parent_key uuid,
    is_folder boolean,
    code text,
    description text,
    other_inc_exp_type text,
    tax_type text,
    tax_accept boolean,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.other_incomes_and_spending OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.other_incomes_and_spending TO gpadmin;




 insert into ods__odata_1c.other_incomes_and_spending
 select * from other_incomes_and_spending_old;
 analyze ods__odata_1c.other_incomes_and_spending;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.positions
 ALTER TABLE ods__odata_1c.positions RENAME TO positions_old;
 




CREATE TABLE ods__odata_1c.positions (
    ref_key uuid,
    description text,
    short_name text,
    add_sort_inf smallint,
    it_park_pers text,
    stat_name text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.positions OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.positions TO gpadmin;




 insert into ods__odata_1c.positions
 select * from positions_old;
 analyze ods__odata_1c.positions;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.receipt_goods_services
 ALTER TABLE ods__odata_1c.receipt_goods_services RENAME TO receipt_goods_services_old;
 




CREATE TABLE ods__odata_1c.receipt_goods_services (
    ref_key uuid,
    data_version text,
    deletion_mark boolean,
    number text,
    date date,
    posted boolean,
    operation_type text,
    organization_key uuid,
    warehouse_key uuid,
    organization_unit_key uuid,
    counterparty_key uuid,
    counterparty_contract_key uuid,
    advance_payment_method text,
    counterparty_accounting_key uuid,
    advance_accounting_key uuid,
    packaging_accounting_key uuid,
    document_currency_key uuid,
    supplier_payment_account_key uuid,
    incoming_document_number text,
    incoming_document_date date,
    shipper_key uuid,
    consignee_key uuid,
    responsible_person_key uuid,
    comment text,
    mutual_settlement_multiplier integer,
    mutual_settlement_rate numeric,
    vat_included_in_cost boolean,
    total_amount_includes_vat boolean,
    document_amount numeric,
    price_type_key uuid,
    manual_correction boolean,
    delete_include_vat boolean,
    delete_invoice_presented boolean,
    delete_incoming_invoice_number text,
    delete_incoming_invoice_date date,
    delete_vat_claimed boolean,
    delete_operation_type_code integer,
    delete_reception_method_code integer,
    transport_type_code text,
    vat_not_separated boolean,
    incoming_ttn_egais_key uuid,
    marked_products_egism boolean,
    representative_key uuid,
    os_location_key uuid,
    asset_group text,
    depreciation_reflection_method_key uuid,
    objects_for_rent boolean,
    excise_tax_accounting boolean,
    excise_included_in_cost boolean,
    bu_depreciation_method text,
    bu_annual_depreciation_rate numeric,
    bu_yearly_depreciation_schedule_key uuid,
    bu_acceleration_coefficient integer,
    bu_output_parameter_key uuid,
    bu_expected_production_volume numeric,
    power_of_attorney text,
    current_power_of_attorney text,
    do_not_reflect_in_vat_register boolean,
    do_not_update_data_when_nomenclature_changes boolean,
    do_not_accept_exchange_changes boolean,
    is_marking boolean,
    expense_article_key uuid,
    cfo_key uuid,
    equipment jsonb,
    construction_objects jsonb,
    services jsonb,
    returnable_packaging jsonb,
    advance_payment_offset jsonb,
    agency_services jsonb,
    fixed_assets jsonb,
    separate_input_vat_accounting jsonb,
    additional_expenses jsonb,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.receipt_goods_services OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.receipt_goods_services TO gpadmin;




 insert into ods__odata_1c.receipt_goods_services
 select * from receipt_goods_services_old;
 analyze ods__odata_1c.receipt_goods_services;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.salary
 ALTER TABLE ods__odata_1c.salary RENAME TO salary_old;
 




CREATE TABLE ods__odata_1c.salary (
    ref_key uuid,
    employee_key uuid,
    department_key uuid,
    payment_key uuid,
    payment numeric,
    workdays smallint,
    workhours numeric,
    start_date date,
    end_date date,
    full_payment numeric,
    currency_key uuid,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.salary OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.salary TO gpadmin;




 insert into ods__odata_1c.salary
 select * from salary_old;
 analyze ods__odata_1c.salary;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.salary_journal
 ALTER TABLE ods__odata_1c.salary_journal RENAME TO salary_journal_old;
 




CREATE TABLE ods__odata_1c.salary_journal (
    ref uuid,
    date timestamp without time zone,
    deletion_mark boolean,
    posted boolean,
    month date,
    paid_sum numeric,
    hold_sum numeric,
    comment text,
    type text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.salary_journal OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.salary_journal TO gpadmin;




 insert into ods__odata_1c.salary_journal
 select * from salary_journal_old;
 analyze ods__odata_1c.salary_journal;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.salary_payrolls
 ALTER TABLE ods__odata_1c.salary_payrolls RENAME TO salary_payrolls_old;
 




CREATE TABLE ods__odata_1c.salary_payrolls (
    ref_key uuid,
    line_number smallint,
    row_identifier uuid,
    employee_key uuid,
    individual_key uuid,
    department_key uuid,
    settlement_period date,
    funding_article_key uuid,
    expense_article_key uuid,
    amount_due numeric,
    salary_delay_compensation numeric,
    account_number text,
    settlement_currency_key uuid,
    amount_due_in_currency numeric,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.salary_payrolls OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.salary_payrolls TO gpadmin;




 insert into ods__odata_1c.salary_payrolls
 select * from salary_payrolls_old;
 analyze ods__odata_1c.salary_payrolls;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.salary_transfers
 ALTER TABLE ods__odata_1c.salary_transfers RENAME TO salary_transfers_old;
 




CREATE TABLE ods__odata_1c.salary_transfers (
    ref_key uuid,
    line_number smallint,
    delete_payroll_key uuid,
    payment_amount numeric,
    payroll uuid,
    payroll_type text,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.salary_transfers OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.salary_transfers TO gpadmin;




 insert into ods__odata_1c.salary_transfers
 select * from salary_transfers_old;
 analyze ods__odata_1c.salary_transfers;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.sick_leaves
 ALTER TABLE ods__odata_1c.sick_leaves RENAME TO sick_leaves_old;
 




CREATE TABLE ods__odata_1c.sick_leaves (
    ref_key uuid,
    employee_key uuid,
    department_key uuid,
    payment_key uuid,
    payment numeric,
    workdays smallint,
    workhours numeric,
    start_date date,
    end_date date,
    deduction_sum numeric,
    deduction_code uuid,
    paid_days integer,
    currency_key uuid,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.sick_leaves OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.sick_leaves TO gpadmin;




 insert into ods__odata_1c.sick_leaves
 select * from sick_leaves_old;
 analyze ods__odata_1c.sick_leaves;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__odata_1c.vacations
 ALTER TABLE ods__odata_1c.vacations RENAME TO vacations_old;
 




CREATE TABLE ods__odata_1c.vacations (
    ref_key uuid,
    employee_key uuid,
    department_key uuid,
    payment_key uuid,
    payment numeric,
    workdays smallint,
    workhours numeric,
    start_date date,
    end_date date,
    deduction_sum numeric,
    deduction_code uuid,
    paid_days integer,
    paid_hours integer,
    currency_key uuid,
    dwh_created_at timestamp with time zone,
    dwh_job_id text
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__odata_1c.vacations OWNER TO gpadmin;


GRANT ALL ON TABLE ods__odata_1c.vacations TO gpadmin;




 insert into ods__odata_1c.vacations
 select * from vacations_old;
 analyze ods__odata_1c.vacations;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_account_plan_item
 ALTER TABLE ods__tranzaxis.avo_account_plan_item RENAME TO avo_account_plan_item_old;
 




CREATE TABLE ods__tranzaxis.avo_account_plan_item (
    guid text,
    ext_guid text,
    inst_id bigint,
    parent_item_guid text,
    code text,
    sub_code text,
    title text,
    synthetic_acct_number text,
    rounding smallint,
    correspondence smallint,
    official smallint,
    kind text,
    has_cur_balance smallint,
    has_balance_hist smallint,
    posting_open_store_days integer,
    posting_close_store_days integer,
    notes text,
    last_update_time timestamp with time zone,
    last_update_user_name text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_account_plan_item OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_account_plan_item TO gpadmin;




 insert into ods__tranzaxis.avo_account_plan_item
 select * from avo_account_plan_item_old;
 analyze ods__tranzaxis.avo_account_plan_item;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__tranzaxis.avo_card уникальное поле:['id'] строк:1342814
 ALTER TABLE ods__tranzaxis.avo_card RENAME TO avo_card_old;
 




CREATE TABLE ods__tranzaxis.avo_card (
    id bigint,
    pin_set_enabled_by_clerk text,
    pin_set_enabled_till timestamp with time zone,
    inst_id bigint,
    pan text,
    pan_hash text,
    pan_crypt text,
    pan_key_id integer,
    mbr smallint,
    design_id integer,
    pvki smallint,
    pvv text,
    ipvv text,
    anti_ambush_pvv text,
    anti_ambush_balance numeric,
    emboss_name text,
    track_name text,
    print_name text,
    pin_block text,
    ipin_block text,
    last_change_pin_tran_id bigint,
    emv_total_cnt_uplmt integer,
    emv_total_cnt_lwlmt integer,
    emv_total_cnt_iclmt integer,
    emv_total_cnt_ilmt integer,
    emv_total_cnt_upilmt integer,
    emv_total_cnt_lwdmstlmt integer,
    emv_total_cnt_updmstlmt integer,
    emv_total_amt_uplmt numeric,
    emv_total_amt_lwlmt numeric,
    emv_total_amt_dclmt numeric,
    emv_sng_tran_dmstlmt numeric,
    emv_vlp_funds_lmt numeric,
    emv_vlp_sng_tran_lmt numeric,
    emv_vlp_reset_threshold numeric,
    emv_app_ccy text,
    last_atc integer,
    is_domestic smallint,
    invalid_cvv2_tries_cnt integer,
    invalid_pin_tries_cnt integer,
    invalid_ipin_tries_cnt integer,
    invalid_cap_tries_cnt integer,
    invalid_tds_enroll_tries_cnt integer,
    last_invalid_cvv2_try_time timestamp with time zone,
    last_invalid_pin_try_time timestamp with time zone,
    last_invalid_ipin_try_time timestamp with time zone,
    last_invalid_cap_try_time timestamp with time zone,
    last_invalid_tds_enroll_try_time timestamp with time zone,
    emv_card_blocked smallint,
    emv_app_blocked smallint,
    track1_dd text,
    track2_dd text,
    use_cap smallint,
    tds_enrollment smallint,
    digitization_eligibility smallint,
    digitization_rid text,
    emv_total_amt_lwlmt2 numeric,
    emv_total_amt_uplmt2 numeric,
    emv_total_cnt_lwlmt2 integer,
    emv_total_cnt_uplmt2 integer,
    emv_app_capabilities integer,
    emv_interface_enabling_switch smallint,
    emv_cvm_limit numeric,
    main_card_app_id bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__tranzaxis.avo_card OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_card TO gpadmin;




 insert into ods__tranzaxis.avo_card
 select * from avo_card_old;
 analyze ods__tranzaxis.avo_card;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_card_product
 ALTER TABLE ods__tranzaxis.avo_card_product RENAME TO avo_card_product_old;
 




CREATE TABLE ods__tranzaxis.avo_card_product (
    id integer,
    network_id integer,
    technology text,
    card_brand text,
    emv_app_label text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_card_product OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_card_product TO gpadmin;




 insert into ods__tranzaxis.avo_card_product
 select * from avo_card_product_old;
 analyze ods__tranzaxis.avo_card_product;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__tranzaxis.avo_contract уникальное поле:['create_tran_id'] строк:4807873
 ALTER TABLE ods__tranzaxis.avo_contract RENAME TO avo_contract_old;
 




CREATE TABLE ods__tranzaxis.avo_contract (
    id bigint,
    class_guid text,
    inst_id bigint,
    branch_id bigint,
    rid text,
    ext_rid text,
    type_id integer,
    client_id bigint,
    title text,
    status text,
    usage_restriction text,
    risk_level integer,
    profile_id bigint,
    main_ccy text,
    is_resident smallint,
    user_attrs text,
    create_time timestamp with time zone,
    create_tran_id bigint,
    create_day date,
    create_user_name text,
    activate_time timestamp with time zone,
    activate_day date,
    activate_user_name text,
    conclusion_day date,
    conclusion_user_name text,
    reconclusion_day date,
    reconclusion_user_name text,
    finish_day date,
    finish_user_name text,
    last_usage_restriction_change_time timestamp with time zone,
    last_usage_restr_change_user_name text,
    close_time timestamp with time zone,
    close_day date,
    close_user_name text,
    notes text,
    last_update_time timestamp with time zone,
    last_update_user_name text,
    hash text,
    ticklered_tran_cnt integer,
    my_entity_guid text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (create_tran_id);


ALTER TABLE ods__tranzaxis.avo_contract OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_contract TO gpadmin;




 insert into ods__tranzaxis.avo_contract
 select * from avo_contract_old;
 analyze ods__tranzaxis.avo_contract;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_contract_type
 ALTER TABLE ods__tranzaxis.avo_contract_type RENAME TO avo_contract_type_old;
 




CREATE TABLE ods__tranzaxis.avo_contract_type (
    id integer,
    parent_id integer,
    is_abstract integer,
    title text,
    last_update_time timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_contract_type OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_contract_type TO gpadmin;




 insert into ods__tranzaxis.avo_contract_type
 select * from avo_contract_type_old;
 analyze ods__tranzaxis.avo_contract_type;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_creditline_contract
 ALTER TABLE ods__tranzaxis.avo_creditline_contract RENAME TO avo_creditline_contract_old;
 




CREATE TABLE ods__tranzaxis.avo_creditline_contract (
    id bigint,
    ccl_base_amt numeric,
    term smallint,
    till_day date,
    cycles_start_day date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_creditline_contract OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_creditline_contract TO gpadmin;




 insert into ods__tranzaxis.avo_creditline_contract
 select * from avo_creditline_contract_old;
 analyze ods__tranzaxis.avo_creditline_contract;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__tranzaxis.avo_creditline_cycle уникальное поле:['created_tran_id'] строк:5169036
 ALTER TABLE ods__tranzaxis.avo_creditline_cycle RENAME TO avo_creditline_cycle_old;
 




CREATE TABLE ods__tranzaxis.avo_creditline_cycle (
    contract_id bigint,
    sd date,
    start_day date,
    dd date,
    grace_day date,
    corp_repay_start_day date,
    used_ccl_sd numeric,
    overlimit_max numeric,
    overlimit_avg numeric,
    overlimit_sd numeric,
    overdue_duration integer,
    overdue_age integer,
    indue_duration integer,
    last_pay_day date,
    pd date,
    daf_day date,
    daf_amt numeric,
    created_tran_id bigint,
    sd_fill_tran_id bigint,
    dd_fill_tran_id bigint,
    daf_amt_fill_tran_id bigint,
    overdue_day date,
    grace_day_print date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (created_tran_id);


ALTER TABLE ods__tranzaxis.avo_creditline_cycle OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_creditline_cycle TO gpadmin;




 insert into ods__tranzaxis.avo_creditline_cycle
 select * from avo_creditline_cycle_old;
 analyze ods__tranzaxis.avo_creditline_cycle;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_creditline_min_pay
 ALTER TABLE ods__tranzaxis.avo_creditline_min_pay RENAME TO avo_creditline_min_pay_old;
 




CREATE TABLE ods__tranzaxis.avo_creditline_min_pay (
    contract_id bigint,
    id integer,
    sd date,
    ccy text,
    debt_kind text,
    debt_cat_id integer,
    debt_classification text,
    balance numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_creditline_min_pay OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_creditline_min_pay TO gpadmin;




 insert into ods__tranzaxis.avo_creditline_min_pay
 select * from avo_creditline_min_pay_old;
 analyze ods__tranzaxis.avo_creditline_min_pay;
end;
$$



DO
$$
begin
 -- Таблица без уникального ключа ods__tranzaxis.avo_creditline_min_pay_delta уникальное поле:MD5(CONCAT(COALESCE("tran_id", 0), COALESCE("min_pay_id", 0), COALESCE("entry_seq", 0)))::uuid строк:5570946
 ALTER TABLE ods__tranzaxis.avo_creditline_min_pay_delta RENAME TO avo_creditline_min_pay_delta_old;
 




CREATE TABLE ods__tranzaxis.avo_creditline_min_pay_delta (
    contract_id bigint,
    min_pay_id integer,
    tran_id bigint,
    entry_seq integer,
    post_day date,
    amt numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
,MD5(CONCAT(COALESCE("tran_id", 0), COALESCE("min_pay_id", 0), COALESCE("entry_seq", 0)))::uuid)  
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1)
 DISTRIBUTED (record_hash);


ALTER TABLE ods__tranzaxis.avo_creditline_min_pay_delta OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_creditline_min_pay_delta TO gpadmin;




 insert into ods__tranzaxis.avo_creditline_min_pay_delta
 select * from avo_creditline_min_pay_delta_old;
 analyze ods__tranzaxis.avo_creditline_min_pay_delta;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_mcc
 ALTER TABLE ods__tranzaxis.avo_mcc RENAME TO avo_mcc_old;
 




CREATE TABLE ods__tranzaxis.avo_mcc (
    id integer,
    title text,
    is_favorite smallint,
    valid_from date,
    valid_to date,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_mcc OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_mcc TO gpadmin;




 insert into ods__tranzaxis.avo_mcc
 select * from avo_mcc_old;
 analyze ods__tranzaxis.avo_mcc;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_network
 ALTER TABLE ods__tranzaxis.avo_network RENAME TO avo_network_old;
 




CREATE TABLE ods__tranzaxis.avo_network (
    id bigint,
    name text,
    code text,
    title text,
    type text,
    corporation_id bigint,
    notes text,
    last_update_time timestamp with time zone,
    last_update_username text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_network OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_network TO gpadmin;




 insert into ods__tranzaxis.avo_network
 select * from avo_network_old;
 analyze ods__tranzaxis.avo_network;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_oper_day
 ALTER TABLE ods__tranzaxis.avo_oper_day RENAME TO avo_oper_day_old;
 




CREATE TABLE ods__tranzaxis.avo_oper_day (
    inst_id bigint,
    day date,
    open_time timestamp with time zone,
    beg_be_current_time timestamp with time zone,
    end_be_current_time timestamp with time zone,
    close_time timestamp with time zone,
    first_tran_id bigint,
    max_tran_id bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_oper_day OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_oper_day TO gpadmin;




 insert into ods__tranzaxis.avo_oper_day
 select * from avo_oper_day_old;
 analyze ods__tranzaxis.avo_oper_day;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_terminal
 ALTER TABLE ods__tranzaxis.avo_terminal RENAME TO avo_terminal_old;
 




CREATE TABLE ods__tranzaxis.avo_terminal (
    id integer,
    status character(1),
    create_time timestamp without time zone,
    activate_time timestamp without time zone,
    last_update_time timestamp with time zone,
    term_type text,
    name text,
    title text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_terminal OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_terminal TO gpadmin;




 insert into ods__tranzaxis.avo_terminal
 select * from avo_terminal_old;
 analyze ods__tranzaxis.avo_terminal;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__tranzaxis.avo_token уникальное поле:['id'] строк:1342814
 ALTER TABLE ods__tranzaxis.avo_token RENAME TO avo_token_old;
 




CREATE TABLE ods__tranzaxis.avo_token (
    id bigint,
    contract_id bigint,
    product_id bigint,
    create_time timestamp with time zone,
    activate_time timestamp with time zone,
    close_time timestamp with time zone,
    valid_time timestamp with time zone,
    exp_time timestamp without time zone,
    life_phase_id integer,
    last_update_time timestamp with time zone,
    kind text,
    status text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (id);


ALTER TABLE ods__tranzaxis.avo_token OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_token TO gpadmin;




 insert into ods__tranzaxis.avo_token
 select * from avo_token_old;
 analyze ods__tranzaxis.avo_token;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_token_life_phase
 ALTER TABLE ods__tranzaxis.avo_token_life_phase RENAME TO avo_token_life_phase_old;
 




CREATE TABLE ods__tranzaxis.avo_token_life_phase (
    id integer,
    rid text,
    inst_id bigint,
    title text,
    acct_map_guid text,
    raw_phase_id integer,
    raw_acct_map_guid text,
    err_acct_map_guid text,
    notes text,
    ext_guid text,
    term_reg_role text,
    user_attrs text,
    account_id bigint,
    raw_account_id bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_token_life_phase OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_token_life_phase TO gpadmin;




 insert into ods__tranzaxis.avo_token_life_phase
 select * from avo_token_life_phase_old;
 analyze ods__tranzaxis.avo_token_life_phase;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__tranzaxis.avo_token_product
 ALTER TABLE ods__tranzaxis.avo_token_product RENAME TO avo_token_product_old;
 




CREATE TABLE ods__tranzaxis.avo_token_product (
    id integer,
    title text,
    last_update_time timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__tranzaxis.avo_token_product OWNER TO gpadmin;


GRANT ALL ON TABLE ods__tranzaxis.avo_token_product TO gpadmin;




 insert into ods__tranzaxis.avo_token_product
 select * from avo_token_product_old;
 analyze ods__tranzaxis.avo_token_product;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.ab_clients
 ALTER TABLE ods__wings.ab_clients RENAME TO ab_clients_old;
 




CREATE TABLE ods__wings.ab_clients (
    exp_id integer,
    test_id integer,
    client_id text,
    date_create timestamp with time zone,
    is_control_group integer,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.ab_clients OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.ab_clients TO gpadmin;




 insert into ods__wings.ab_clients
 select * from ab_clients_old;
 analyze ods__wings.ab_clients;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.ab_test
 ALTER TABLE ods__wings.ab_test RENAME TO ab_test_old;
 




CREATE TABLE ods__wings.ab_test (
    test_id integer,
    t_version numeric,
    date_start timestamp with time zone,
    date_end timestamp with time zone,
    identifier_desc text,
    test_group numeric,
    control_group numeric,
    test_desc text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.ab_test OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.ab_test TO gpadmin;




 insert into ods__wings.ab_test
 select * from ab_test_old;
 analyze ods__wings.ab_test;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.batch
 ALTER TABLE ods__wings.batch RENAME TO batch_old;
 




CREATE TABLE ods__wings.batch (
    id bigint,
    name text,
    date_create timestamp with time zone,
    date_apply timestamp with time zone,
    user_login_create text,
    user_login_apply text,
    comment text,
    is_applied smallint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.batch OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.batch TO gpadmin;




 insert into ods__wings.batch
 select * from batch_old;
 analyze ods__wings.batch;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.card_history
 ALTER TABLE ods__wings.card_history RENAME TO card_history_old;
 




CREATE TABLE ods__wings.card_history (
    reference bigint,
    request_id text,
    card_id text,
    date_create timestamp with time zone,
    payment_system text,
    operation_history jsonb,
    cached integer,
    errors jsonb,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.card_history OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.card_history TO gpadmin;




 insert into ods__wings.card_history
 select * from card_history_old;
 analyze ods__wings.card_history;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__wings.cases уникальное поле:['reference'] строк:1131950
 ALTER TABLE ods__wings.cases RENAME TO cases_old;
 




CREATE TABLE ods__wings.cases (
    reference bigint,
    request_id uuid,
    user_id text,
    date_create timestamp with time zone,
    date_update timestamp with time zone,
    user_name text,
    feature text,
    request_type text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (reference);


ALTER TABLE ods__wings.cases OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.cases TO gpadmin;




 insert into ods__wings.cases
 select * from cases_old;
 analyze ods__wings.cases;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__wings.cases_attr уникальное поле:['reference'] строк:1038803
 ALTER TABLE ods__wings.cases_attr RENAME TO cases_attr_old;
 




CREATE TABLE ods__wings.cases_attr (
    reference bigint,
    client_id text,
    application_id uuid,
    application_dttm timestamp with time zone,
    application_type text,
    pilot_id text,
    income_inps_12m_avg numeric,
    conf_income_median numeric,
    conf_income_1_m_ago numeric,
    conf_income_2_m_ago numeric,
    conf_income_3_m_ago numeric,
    conf_income_4_m_ago numeric,
    conf_income_5_m_ago numeric,
    conf_income_6_m_ago numeric,
    conf_income_7_m_ago numeric,
    conf_income_8_m_ago numeric,
    conf_income_9_m_ago numeric,
    conf_income_10_m_ago numeric,
    conf_income_11_m_ago numeric,
    conf_income_12_m_ago numeric,
    limit_max_allowed numeric,
    limit_offered numeric,
    decision text,
    decision_dttm timestamp with time zone,
    pd_model_id text,
    pd_model_score numeric,
    pd_90dpd numeric,
    income_model_id text,
    income_model_output numeric,
    income_used numeric,
    scoring_reject_flag integer,
    initiated_by text,
    credit_burden bigint,
    total_debt bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (reference);


ALTER TABLE ods__wings.cases_attr OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.cases_attr TO gpadmin;




 insert into ods__wings.cases_attr
 select * from cases_attr_old;
 analyze ods__wings.cases_attr;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.guides
 ALTER TABLE ods__wings.guides RENAME TO guides_old;
 




CREATE TABLE ods__wings.guides (
    reference bigint,
    date_create timestamp with time zone,
    date_update timestamp with time zone,
    guide_id bigint,
    code smallint,
    description text,
    intval1 smallint,
    intval2 smallint,
    intval3 smallint,
    strval1 text,
    strval2 text,
    strval3 text,
    floatval1 real,
    floatval2 real,
    floatval3 real,
    boolval1 boolean,
    boolval2 boolean,
    boolval3 boolean,
    deadline interval,
    user_added text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.guides OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.guides TO gpadmin;




 insert into ods__wings.guides
 select * from guides_old;
 analyze ods__wings.guides;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.guides_name
 ALTER TABLE ods__wings.guides_name RENAME TO guides_name_old;
 




CREATE TABLE ods__wings.guides_name (
    reference bigint,
    date_create timestamp with time zone,
    date_update timestamp with time zone,
    guide_id bigint,
    guide_name text,
    description text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.guides_name OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.guides_name TO gpadmin;




 insert into ods__wings.guides_name
 select * from guides_name_old;
 analyze ods__wings.guides_name;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.im_predictions
 ALTER TABLE ods__wings.im_predictions RENAME TO im_predictions_old;
 




CREATE TABLE ods__wings.im_predictions (
    reference bigint,
    request_id text,
    client_id text,
    date_create timestamp with time zone,
    lower numeric,
    mean numeric,
    upper numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.im_predictions OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.im_predictions TO gpadmin;




 insert into ods__wings.im_predictions
 select * from im_predictions_old;
 analyze ods__wings.im_predictions;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.model_resolve
 ALTER TABLE ods__wings.model_resolve RENAME TO model_resolve_old;
 




CREATE TABLE ods__wings.model_resolve (
    application_id text,
    client_id text,
    application_date text,
    age numeric,
    region_my_id text,
    loans_num smallint,
    trace_id uuid,
    income_12m_avg numeric,
    income_norm_std numeric,
    woe_age numeric,
    woe_region_my_id numeric,
    woe_ch_score numeric,
    woe_delq_max_dpd_l3m numeric,
    woe_loans_num numeric,
    woe_requests_num_l2w numeric,
    woe_income_zero_months numeric,
    woe_income_12m_avg numeric,
    woe_income_norm_std numeric,
    tech_pd numeric,
    score numeric,
    ch_score integer,
    delq_max_dpd_l3m smallint,
    requests_num_l2w smallint,
    income_zero_months smallint,
    profit_flg boolean,
    profit_reject_flg boolean,
    is_approved_flg boolean,
    reference bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.model_resolve OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.model_resolve TO gpadmin;




 insert into ods__wings.model_resolve
 select * from model_resolve_old;
 analyze ods__wings.model_resolve;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.offers
 ALTER TABLE ods__wings.offers RENAME TO offers_old;
 




CREATE TABLE ods__wings.offers (
    reference bigint,
    request_id uuid,
    offer_id uuid,
    user_id uuid,
    valid_from bigint,
    valid_to bigint,
    date_create timestamp with time zone,
    max_limit bigint,
    nc_offer bigint,
    pdn_profit bigint,
    expense_amount bigint,
    total_debt bigint,
    process_id bigint,
    pti_median numeric,
    pti_pdn numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.offers OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.offers TO gpadmin;




 insert into ods__wings.offers
 select * from offers_old;
 analyze ods__wings.offers;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.r_par
 ALTER TABLE ods__wings.r_par RENAME TO r_par_old;
 




CREATE TABLE ods__wings.r_par (
    id bigint,
    code text,
    name text,
    is_input smallint,
    is_tab smallint,
    is_calc smallint,
    is_critical smallint,
    is_final_pre smallint,
    is_final smallint,
    is_output smallint,
    script text,
    r_par_calc_type_code text,
    r_par_datatype_code text,
    batch_id bigint,
    external_format text,
    status smallint,
    status_new smallint,
    arr_type smallint,
    json_type smallint,
    is_critical_rejecting smallint,
    is_const smallint,
    value text,
    is_not_saved smallint,
    is_encrypted smallint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.r_par OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.r_par TO gpadmin;




 insert into ods__wings.r_par
 select * from r_par_old;
 analyze ods__wings.r_par;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.r_process
 ALTER TABLE ods__wings.r_process RENAME TO r_process_old;
 




CREATE TABLE ods__wings.r_process (
    id bigint,
    name text,
    code text,
    descr text,
    strategy_id bigint,
    rs_process_type_code text,
    requests_percentage numeric,
    compilation_id integer,
    is_external smallint,
    is_active smallint,
    batch_id bigint,
    status smallint,
    status_new smallint,
    date_insert timestamp with time zone,
    user_login text,
    time_out integer,
    response_wrapper_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.r_process OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.r_process TO gpadmin;




 insert into ods__wings.r_process
 select * from r_process_old;
 analyze ods__wings.r_process;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.r_process_object
 ALTER TABLE ods__wings.r_process_object RENAME TO r_process_object_old;
 




CREATE TABLE ods__wings.r_process_object (
    id bigint,
    r_process_code text,
    workflow_object_code text,
    workflow_object_type text,
    batch_id bigint,
    status smallint,
    status_new smallint,
    is_first_object smallint,
    position_x integer,
    position_y integer,
    external_code text,
    in_par_path text,
    out_par_path text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.r_process_object OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.r_process_object TO gpadmin;




 insert into ods__wings.r_process_object
 select * from r_process_object_old;
 analyze ods__wings.r_process_object;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.r_process_object_next
 ALTER TABLE ods__wings.r_process_object_next RENAME TO r_process_object_next_old;
 




CREATE TABLE ods__wings.r_process_object_next (
    id bigint,
    workflow_object_next_code text,
    workflow_object_next_type text,
    workflow_object_code text,
    batch_id bigint,
    status smallint,
    status_new smallint,
    r_process_code text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.r_process_object_next OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.r_process_object_next TO gpadmin;




 insert into ods__wings.r_process_object_next
 select * from r_process_object_next_old;
 analyze ods__wings.r_process_object_next;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__wings.reports_request уникальное поле:['reference'] строк:1323629
 ALTER TABLE ods__wings.reports_request RENAME TO reports_request_old;
 




CREATE TABLE ods__wings.reports_request (
    reference bigint,
    request_id uuid,
    report_type smallint,
    date_create timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 DISTRIBUTED BY (request_id);


ALTER TABLE ods__wings.reports_request OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.reports_request TO gpadmin;




 insert into ods__wings.reports_request
 select * from reports_request_old;
 analyze ods__wings.reports_request;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__wings.rout уникальное поле:['reference'] строк:5066284
 ALTER TABLE ods__wings.rout RENAME TO rout_old;
 




CREATE TABLE ods__wings.rout (
    reference bigint,
    entity_id uuid,
    entity_key text,
    rout_point text,
    date_create timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED (reference);


ALTER TABLE ods__wings.rout OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.rout TO gpadmin;




 insert into ods__wings.rout
 select * from rout_old;
 analyze ods__wings.rout;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.rwa
 ALTER TABLE ods__wings.rwa RENAME TO rwa_old;
 




CREATE TABLE ods__wings.rwa (
    reference bigint,
    request_id uuid,
    user_id uuid,
    offer_id uuid,
    date_create timestamp with time zone,
    total_debt numeric,
    total_payment numeric,
    profit numeric,
    pti_with_avo numeric,
    pti_without_avo numeric,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.rwa OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.rwa TO gpadmin;




 insert into ods__wings.rwa
 select * from rwa_old;
 analyze ods__wings.rwa;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.tasks_req
 ALTER TABLE ods__wings.tasks_req RENAME TO tasks_req_old;
 




CREATE TABLE ods__wings.tasks_req (
    reference bigint,
    request_id text,
    client_id text,
    task_id text,
    task_type text,
    date_create timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 DISTRIBUTED BY (task_id);


ALTER TABLE ods__wings.tasks_req OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.tasks_req TO gpadmin;




 insert into ods__wings.tasks_req
 select * from tasks_req_old;
 analyze ods__wings.tasks_req;
end;
$$



DO
$$
begin
 -- Таблица c уникальным ключом ods__wings.tasks_res уникальное поле:['reference'] строк:3307189
 ALTER TABLE ods__wings.tasks_res RENAME TO tasks_res_old;
 




CREATE TABLE ods__wings.tasks_res (
    reference bigint,
    request_id text,
    client_id text,
    task_id text,
    task_type text,
    status text,
    reject_reason text,
    date_create timestamp with time zone,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 DISTRIBUTED BY (task_id);


ALTER TABLE ods__wings.tasks_res OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.tasks_res TO gpadmin;




 insert into ods__wings.tasks_res
 select * from tasks_res_old;
 analyze ods__wings.tasks_res;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.temp_users_checked
 ALTER TABLE ods__wings.temp_users_checked RENAME TO temp_users_checked_old;
 




CREATE TABLE ods__wings.temp_users_checked (
    user_id uuid,
    date_updated timestamp with time zone,
    resend_status text,
    reference bigint,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.temp_users_checked OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.temp_users_checked TO gpadmin;




 insert into ods__wings.temp_users_checked
 select * from temp_users_checked_old;
 analyze ods__wings.temp_users_checked;
end;
$$



DO
$$
begin
 -- Маленькая непонятная таблица ods__wings.users_expired_consents
 ALTER TABLE ods__wings.users_expired_consents RENAME TO users_expired_consents_old;
 




CREATE TABLE ods__wings.users_expired_consents (
    reference bigint,
    user_id uuid,
    date_updated timestamp with time zone,
    resend_status text,
    dwh_job_id text,
    dwh_created_at timestamp with time zone
)
WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__wings.users_expired_consents OWNER TO gpadmin;


GRANT ALL ON TABLE ods__wings.users_expired_consents TO gpadmin;




 insert into ods__wings.users_expired_consents
 select * from users_expired_consents_old;
 analyze ods__wings.users_expired_consents;
end;
$$


