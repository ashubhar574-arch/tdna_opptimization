# Databricks notebook source
dbutils.widgets.text("metadata_table_path","/Volumes/ops_catalog/cxops/edpppecops_config/metadata_delta")
metadata_table_path = dbutils.widgets.get("metadata_table_path")

# COMMAND ----------

import json
from pyspark.sql import Row
from pyspark.sql.functions import *
import traceback
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, TimestampType, DoubleType, LongType
from datetime import datetime, timedelta, timezone

df = spark.read.load(metadata_table_path).filter("module_name = 'TURNAROUND_INFO' and status = 'Active' ").orderBy(col("insertion_datetime").desc()).limit(1)

config = df.first()

db_name = config['cosmos_database'] 
container_name = config['cosmos_container'] 
eventhub_namespace = config['eventhub_namespace']
eventhub_name = config['eventhub_name']
eh_policy_name = config['eventhub_policy_name']
eventhub_key_name = config['eventhub_key_name']
module_name = config['module_name']
ehscope = config['env_scope'] 
env_scope=config['env_scope'] 
storage_key_name = config['storage_key_name']
cosmos_sec_key = config['cosmos_key_name'] 
cosmos_endpoint = config['cosmos_endpoint_key'] 
checkpoint_dir = config['checkpoint_dir']
raw_dir = config['raw_dir']
product_dir = config['product_dir']
metadata_dir = config['metadata_dir']
logging_dir = config['logging_dir']
eventhub_consumer_group = config['eventhub_consumer_group']
conops_volume = config['conops_volume']

deltaFolderCheckpoint = checkpoint_dir 
deltaFolderMain = raw_dir 
product_path = product_dir

exception_log_delta_path = logging_dir + 'exception_logs'
batch_log_delta_path = logging_dir + 'batch_logs'
log_directory = logging_dir
project_name="avtura_turnaround"
logfile_name="avtura_turnaround_logs"