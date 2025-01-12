import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import *


jars_dir = r'/Users/avotech/drivers/mssql'
jdbc_driver_path = [os.path.join(jars_dir, i) for i in os.listdir(jars_dir)]
jdbc_driver_path = ",".join(jdbc_driver_path)
source_table = "kafka.tx_events"
target_table = "temp.tx_events_hist"
# spark = SparkSession.builder \
#     .appName("MSSQL to PostgreSQL Transfer") \
#     .getOrCreate()

POSTGRES_URL = "jdbc:postgresql://10.122.3.134:5432/greenplum-dwh"
POSTGRES_USER = "gpadmin"
POSTGRES_PASSWORD = ""
POSTGRES_TABLE = "temp.tx_events_hist"
JDBC_DRIVER_PATH = "/Users/avotech/drivers/postgresql-42.7.4.jar"  # Update with your JDBC driver path

# Define schema for the DataFrame
schema = StructType([
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", StringType(), True),
    StructField("kafka_offset", StringType(), True),
    StructField("kafka_created_at", StringType(), True),
    StructField("kafka_value", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("context_status_cl", StringType(), True),
    StructField("context_status_rp", StringType(), True),
    StructField("context_person_id", StringType(), True),
    StructField("context_core_bank_client_id", StringType(), True),
    StructField("context_credit_report_type", StringType(), True),
    StructField("context_credit_report_body_report", StringType(), True),
    StructField("context_cr_request_id", StringType(), True),
    StructField("context_reject_reason_exp", StringType(), True),
    StructField("context_acquier", StringType(), True),
    StructField("context_tx_transaction_id", StringType(), True),
    StructField("context_mcc", StringType(), True),
    StructField("context_masked_card_number", StringType(), True),
    StructField("context_merchant_name", StringType(), True),
    StructField("context_tran_time", StringType(), True),
    StructField("context_reject_reason", StringType(), True),
    StructField("context_is_partial", StringType(), True),
    StructField("context_commission", StringType(), True),
    StructField("context_original_currency", StringType(), True),
    StructField("context_terminal_desc", StringType(), True),
    StructField("context_specification_id", StringType(), True),
    StructField("context_terminal_coutry", StringType(), True),
    StructField("context_amount", StringType(), True),
    StructField("context_product_id", StringType(), True),
    StructField("context_vat", StringType(), True),
    StructField("context_is_advice", StringType(), True),
    StructField("context_is_reversal", StringType(), True),
    StructField("context_parent_transaction_id", StringType(), True),
    StructField("context_reg_time", StringType(), True),
    StructField("context_operation_trace_id", StringType(), True),
    StructField("context_original_amount", StringType(), True),
    StructField("context_terminal_city", StringType(), True),
    StructField("context_card_id", StringType(), True),
    StructField("context_ichg_fee_amt", StringType(), True),
    StructField("context_life_phase", StringType(), True),
    StructField("context_ichg_fee_currency", StringType(), True),
    StructField("context_status", StringType(), True),
    StructField("context_tx_command_id", StringType(), True),
    StructField("context_tx_id", StringType(), True),
    StructField("context_requested_cl", StringType(), True),
    StructField("context_applied_cl", StringType(), True),
    StructField("context_user_id", StringType(), True),
    StructField("context_transaction_id", StringType(), True),
    StructField("context_loan_id", StringType(), True),
    StructField("context_bankomat_id", StringType(), True),
    StructField("error_error_code", StringType(), True),
    StructField("error_error_description", StringType(), True),
    StructField("dwh_created_at", StringType(), True),
    StructField("dwh_job_id", StringType(), True)
])

print('fffdg')
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Load CSV to PostgreSQL") \
    .master("local[*]") \
    .config("spark.jars", JDBC_DRIVER_PATH) \
    .config("spark.executor.heartbeatInterval", "200s") \
    .config("spark.network.timeout", "300s") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.driver.memory", "2G") \
    .config("spark.executor.memory", "2G") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", r"/Users/avotech/00/spark_log") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Load CSV data into a DataFrame
csv_file_path = r"/Users/avotech/00/tx_events_hist.csv"  # Update with your CSV file path
df = spark.read.csv(csv_file_path, schema=schema, header=True)

# Write DataFrame to PostgreSQL
df.write \
    .format("jdbc") \
    .option("url", POSTGRES_URL) \
    .option("dbtable", POSTGRES_TABLE) \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("Data loaded successfully into PostgreSQL.")

# Stop SparkSession
spark.stop()


conf = SparkConf().setMaster("local[*]").set("spark.sql.debug.maxToStringFields", 1000) \
                                    .setMaster("local[*]") \
                                    .set("spark.executor.heartbeatInterval", 200000) \
                                    .set("spark.network.timeout", 300000) \
                                    .set("spark.sql.execution.arrow.pyspark.enabled", "true") \
                                    .set("spark.jars", jdbc_driver_path) \
                                    .set("spark.ui.port", 4040) \
                                    .set("spark.driver.cores", "5") \
                                    .set("spark.executor.cores", "5") \
                                    .set("spark.driver.memory", "1G")  \
                                    .set("spark.executor.memory", "1G")  \
                                    .set("spark.executor.instances", "2") \
                                    .setAppName("PYSPARK_MSSQL_TUTORIAL")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
numPartitions = 10
fetchsize = 3000
batchsize = 3000

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
jdbc_url = "jdbc:sqlserver://{};databaseName={};user={};password={};".format('10.124.0.20', 'ods_chiltan', 'etl_login', '')

bound_query = f"(SELECT ROW_NUMBER() OVER (ORDER BY username) AS row_num FROM {source_table}) as my_table"
bound_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", bound_query) \
    .option("driver", driver) \
    .load()

url = 'jdbc:sqlserver://10.124.0.20:1433' + ";" + "databaseName=" + 'ods_chiltan' + ";"
jdbcDF = spark.read \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .option("url", url) \
    .option("dbtable", source_table) \
    .option("user", 'etl_login') \
    .option("password", '""').load()

# JDBC Connection Details
mssql_url = "jdbc:sqlserver://10.124.0.20:1433;databaseName=ods_chiltan"
mssql_properties = {
    "user": "etl_login",
    "password": "ww1vt2lDERozC9C4kH3XCgakjFYb7iYS",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

postgres_url = "jdbc:postgresql://0.122.3.134:5432/greenplum-dwh"
postgres_properties = {
    "user": "gpadmin",
    "password": "",
    "driver": "org.postgresql.Driver"
}

# Table Names


postgres_url = "jdbc:postgresql://0.122.3.134:5432/greenplum-dwh"
postgres_properties = {
    "user": os.getenv("POSTGRES_USER", "gpadmin"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "driver": "org.postgresql.Driver"
}


# Step 1: Read Data from MSSQL
print(f"Reading data from MSSQL table: {source_table}")
mssql_df = spark.read.jdbc(
    url=mssql_url,
    table=source_table,
    properties=mssql_properties
)

# Optional: Show the data read from MSSQL
mssql_df.show()

# Step 2: Write Data to PostgreSQL
print(f"Writing data to PostgreSQL table: {target_table}")
mssql_df.write.jdbc(
    url=postgres_url,
    table=target_table,
    mode="overwrite",  # Options: "append", "overwrite", "ignore", "error"
    properties={**postgres_properties, "batchsize": "10000"}  # Optional batch size
)

print("Data transfer completed successfully!")

# Stop Spark Session
spark.stop()

