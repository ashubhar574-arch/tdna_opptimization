# Databricks notebook source
# MAGIC %run "/Workspace/Users/eyakankshar@etihadppe.ae/guest_preference_v1/guesthub/guest_profile/notebooks/configuration_setup"

# COMMAND ----------

dbutils.widgets.text("metadata_table_path","/Volumes/ops_catalog/cxops/edpppecops_config/metadata_delta/")
metadata_table_path = dbutils.widgets.get("metadata_table_path")

# COMMAND ----------

import json
from datetime import datetime, timezone
from pyspark.sql.functions import *

meta_data = spark.read.load(metadata_table_path).filter("module_name = 'GUEST_PROFILE' and status = 'Active' ").orderBy(col("insertion_datetime").desc()).limit(1)
config = meta_data.first()

cosmos_database        = "GUESTHUBDB-DEV"
cosmos_container       = config['cosmos_container']
eventhub_namespace     = config['eventhub_namespace']
eventhub_name          = config['eventhub_name']
eventhub_policy        = config['eventhub_policy_name']
eventhub_key_name      = config['eventhub_key_name']
module_name            = config['module_name']
env_scope              = config['env_scope']
storage_acc_name       = config['storage_acc_name']
storage_key_name       = config['storage_key_name']
cosmos_key_name        = config['cosmos_key_name']
cosmos_endpoint_key    = config['cosmos_endpoint_key']
checkpoint_dir         = "/Volumes/ops_catalog/cxops/edpppecops_raw/eag/ey/guest_hub/checkpoints/guest_preference_checkpoint/"
raw_dir                = config['raw_dir']
product_dir            = config['product_dir']
logging_dir            = config['logging_dir']
eventhub_consumer_group = config['eventhub_consumer_group']
conops_volume          = config['conops_volume']
logic_app_url          = config['logic_app_url']
maxoffset = config["kafka_maxOffsetsPerTrigger"]
startingoffsets = config["kafka_startingOffsets"]
eventhub_consumer_group = config["eventhub_consumer_group"]

# Secrets
eventhub_key   = dbutils.secrets.get(scope=env_scope, key=eventhub_key_name)
cosmosMasterKey = dbutils.secrets.get(scope=env_scope, key=cosmos_key_name)
cosmosEndpoint  = dbutils.secrets.get(scope=env_scope, key=cosmos_endpoint_key)

# Cosmos config via ConfigManager
cosmos_config, cosmos_format = ConfigManager.get_cosmos_config(
    env_scope=env_scope,
    cosmos_endpoint=cosmos_endpoint_key,
    cosmos_sec_key=cosmos_key_name,
    db_name=cosmos_database,
    container_name=cosmos_container
)

# EventHub config via ConfigManager
ehConf = ConfigManager.get_eventhub_config(
    env_scope,
    eventhub_name,
    eventhub_namespace,
    eventhub_key_name,
    eventhub_policy,
    maxoffset=1000,
    consumer_group = eventhub_consumer_group
)

KAFKA_OPTIONS = ConfigManager.get_kafka_connector_config(env_scope,eventhub_namespace, eventhub_name, eventhub_policy, eventhub_key_name, eventhub_consumer_group, maxoffset, startingoffsets)

print("Cosmos config and EventHub config initialized")