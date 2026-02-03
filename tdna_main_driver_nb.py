# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, TimestampType, DoubleType, LongType
from pyspark.sql.functions import expr, current_timestamp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, from_json, get_json_object, lit, expr
from pyspark.sql.functions import expr, current_timestamp
from pyspark.sql import Row
import traceback
import uuid
from datetime import datetime
import time
import sys
import os
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lower, filter as spark_filter
from pyspark.sql.functions import (
    col, explode_outer, row_number, lit, collect_set, struct,
    from_json, concat_ws, array, expr, lower, to_date, when, date_format, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import traceback
import time
from pyspark.sql import SparkSession
from datetime import datetime, timezone
from pyspark.sql.functions import broadcast

# COMMAND ----------

# --- GC & memory pressure tuning ---
spark.conf.set("spark.sql.shuffle.partitions", "120")  # 200 is too high for streaming
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.databricks.sql.eagerShuffleEval.enabled", "false")
spark.conf.set("spark.streaming.backpressure.enabled", "true")
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "2000")

# Reduce driver memory churn
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.codegen.wholeStage", "true")


# COMMAND ----------

# MAGIC %run "/Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_profile/notebooks/tdna_config_nb"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_profile/notebooks/tdna_schema_nb"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_profile/notebooks/tdna_data_processing_nb"

# COMMAND ----------

# MAGIC %run "/Workspace/main/common_utilities/logger"

# COMMAND ----------

project_name="TDNA_Processor"
logfile_name="tdna_processing_"

# COMMAND ----------

dbutils.widgets.text("bridge_table_path","")
bridge_table_path = dbutils.widgets.get("bridge_table_path")

# COMMAND ----------

from delta.tables import DeltaTable
import traceback

log_directory_rejected_rows = logging_dir + 'rejected_rows_logs'
log_directory_batch = logging_dir + 'batch_logs'
log_directory_exception = logging_dir + 'exception_logs'
product_path = product_dir

project_name = "TDNA_Processor"
logfile_name = "tdna_processing_"

try:
    logger = applogger(log_directory_rejected_rows, project_name, logfile_name)
except Exception as e:
    print("Error while creating logger object: \n{}".format(e))
    sys.exit(1)

spark.sparkContext.setLogLevel("WARN")

ConfigManager.get_adls_config(env_scope, storage_key_name, storage_acc_name)

def process_batch(batch_df, batch_id):
    """
    Wrapper function for stream processing
    Calls the main batch processor from data_processing_nb
    """
    return process_batch_with_dependencies(
        batch_df=batch_df,
        batch_id=batch_id,
        raw_dir=raw_dir,
        product_dir=product_dir,
        bridge_table_path=bridge_table_path,
        json_schema=json_schema,
        cosmos_format=cosmos_format,
        cosmos_config=cosmos_config,
        log_directory_batch=log_directory_batch,
        log_directory_exception=log_directory_exception,
        logger=logger
    )
start_time_global = time.time()
try:
    for q in spark.streams.active:
        if q.name == "process_tdna_data":
            q.stop()

    df_eh = (spark.readStream
              .format("eventhubs")
              .options(**ehConf)
              .load()
              .withColumn("json_body", expr("CAST(Body as string)"))
              .withColumn("ingestiontime",current_timestamp())
              .withColumn("time_difference_seconds", expr("unix_timestamp(ingestiontime) - unix_timestamp(enqueuedTime)"))
              .select("json_body", "ingestiontime", "enqueuedTime","time_difference_seconds", "sequenceNumber"))

    streaming_query = (df_eh.writeStream
        .queryName("process_tdna_data")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime='15 seconds')
        .foreachBatch(process_batch)
        .start())

    streaming_query.awaitTermination()

except Exception as e:
    err_msg = f"Fatal error in main execution: {str(e)}"
    logger.error(err_msg)
    logger.error(traceback.format_exc())
    total_dur = time.time() - start_time_global
    raise