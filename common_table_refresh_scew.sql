
DO
$$
begin
 -- Таблица справочник datamart_cod.tx_fido_oper_day row_count = 1892
 drop table if exists datamart_cod.tx_fido_oper_day_old;
 ALTER TABLE datamart_cod.tx_fido_oper_day RENAME TO tx_fido_oper_day_old;
 




CREATE TABLE datamart_cod.tx_fido_oper_day (
    oper_day date ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    from_day date ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    dwh_job_id text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    dwh_created_at timestamp with time zone ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768)
)
-- WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE datamart_cod.tx_fido_oper_day OWNER TO gpadmin;


GRANT ALL ON TABLE datamart_cod.tx_fido_oper_day TO gpadmin;




 insert into datamart_cod.tx_fido_oper_day
 select t.* from datamart_cod.tx_fido_oper_day_old t;
 analyze datamart_cod.tx_fido_oper_day;
end;
$$
                


DO
$$
begin
 -- Таблица справочник ods__fido_cod.day_operational_cod row_count = 1893
 drop table if exists ods__fido_cod.day_operational_cod_old;
 ALTER TABLE ods__fido_cod.day_operational_cod RENAME TO day_operational_cod_old;
 




CREATE TABLE ods__fido_cod.day_operational_cod (
    prev_oper_day date ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    oper_day date ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    next_oper_day date ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    day_status smallint ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    dwh_job_id text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    dwh_created_at timestamp with time zone ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768)
)
-- WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1')
 WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED REPLICATED;


ALTER TABLE ods__fido_cod.day_operational_cod OWNER TO gpadmin;


GRANT ALL ON TABLE ods__fido_cod.day_operational_cod TO gpadmin;




 insert into ods__fido_cod.day_operational_cod
 select t.* from ods__fido_cod.day_operational_cod_old t;
 analyze ods__fido_cod.day_operational_cod;
end;
$$
                


DO
$$
begin
 -- Маленькая непонятная таблица ods__ktc.overall_unavailable_reasons_p row_count = 13199
 drop table if exists ods__ktc.overall_unavailable_reasons_p_old;
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
-- WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1', blocksize='32768')
 DISTRIBUTED RANDOMLY;


ALTER TABLE ods__ktc.overall_unavailable_reasons_p OWNER TO gpadmin;


GRANT ALL ON TABLE ods__ktc.overall_unavailable_reasons_p TO gpadmin;




 insert into ods__ktc.overall_unavailable_reasons_p
 select t.* from ods__ktc.overall_unavailable_reasons_p_old t;
 analyze ods__ktc.overall_unavailable_reasons_p;
end;
$$
                


DO
$$
begin
 -- Маленькая непонятная таблица datamart_risks.income_model_monitoring row_count = 62231
 drop table if exists datamart_risks.income_model_monitoring_old;
 ALTER TABLE datamart_risks.income_model_monitoring RENAME TO income_model_monitoring_old;
 




CREATE TABLE datamart_risks.income_model_monitoring (
    application_id text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    user_id text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    application_dttm timestamp with time zone ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    iteration_number bigint ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    region_id numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    gender numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    age numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    is_ru_lang numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    cards_count numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    avg_hsam_dt numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    avg_hscnt_dt numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    avg_uh_p2p_amnt_dt numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    avg_uh_finance_amnt_dt numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    avg_uh_retail_amnt_dt numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    avg_uh_others_amnt_dt numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    avg_uh_transport_amnt_dt numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    features_adequacy integer ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    reconstruction_usefulness integer ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    income_lower numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    income_mean numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    income_upper numeric ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    status text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768)
)
-- WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1', blocksize='32768')
 DISTRIBUTED RANDOMLY;


ALTER TABLE datamart_risks.income_model_monitoring OWNER TO gpadmin;


GRANT ALL ON TABLE datamart_risks.income_model_monitoring TO gpadmin;




 insert into datamart_risks.income_model_monitoring
 select t.* from datamart_risks.income_model_monitoring_old t;
 analyze datamart_risks.income_model_monitoring;
end;
$$
                


DO
$$
begin
 -- Таблица c уникальным ключом ods__asterisk.agents_timetable уникальное поле:['id'] строк:21911
 
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
-- WITH (appendonly='true', orientation='row', compresstype=zstd, compresslevel='1')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED BY (id); --  (id);


ALTER TABLE ods__asterisk.agents_timetable OWNER TO gpadmin;


GRANT ALL ON TABLE ods__asterisk.agents_timetable TO gpadmin;




 insert into ods__asterisk.agents_timetable
 select t.* from ods__asterisk.agents_timetable_old t;
 analyze ods__asterisk.agents_timetable;
end;
$$
                


DO
$$
begin
 -- Таблица c уникальным ключом ods__jira.tickets_custex уникальное поле:['id'] строк:35506
 
 ALTER TABLE ods__jira.tickets_custex RENAME TO tickets_custex_old;
 




CREATE TABLE ods__jira.tickets_custex (
    id bigint ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    issue_key text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    status text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    assignee text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    component text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    created timestamp with time zone ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    dwh_job_id text ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768),
    dwh_created_at timestamp with time zone ENCODING (compresstype=zstd,compresslevel=1,blocksize=32768)
)
-- WITH (appendonly='true', orientation='column', compresstype=zstd, compresslevel='1')
 
WITH (appendonly = true,
orientation = column,
compresstype = zstd,
compresslevel = 1) 
 DISTRIBUTED BY (id); --  (id);


ALTER TABLE ods__jira.tickets_custex OWNER TO gpadmin;


GRANT ALL ON TABLE ods__jira.tickets_custex TO gpadmin;




 insert into ods__jira.tickets_custex
 select t.* from ods__jira.tickets_custex_old t;
 analyze ods__jira.tickets_custex;
end;
$$
                

