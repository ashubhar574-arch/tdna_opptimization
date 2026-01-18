# Databricks notebook source
# MAGIC %run "/Workspace/Shared/common_utilities/CONFIG_MANAGER"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/eyakankshar@etihadppe.ae/EDP2-COPS_turnaround_info/flighthub/turnaround_info/notebooks/avtura_config_nb"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/eyakankshar@etihadppe.ae/EDP2-COPS_turnaround_info/flighthub/turnaround_info/notebooks/avtura_schema_nb"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/eyakankshar@etihadppe.ae/EDP2-COPS_turnaround_info/flighthub/turnaround_info/notebooks/avtura_data_processing_nb"

# COMMAND ----------

# MAGIC %run "/Workspace/Shared/common_utilities/logger"

# COMMAND ----------

from pyspark.sql.functions import get_json_object, col, current_timestamp
import pytz
from datetime import datetime

logger = applogger(log_directory, "eventhub_streaming", "streaming_")
try:
    ehConf = ConfigManager.get_eventhub_config(ehscope, eventhub_name=eventhub_name, eventhub_namespace=eventhub_namespace, ehkey=eventhub_key_name, eh_policy_name=eh_policy_name, maxoffset=1000)
except Exception as e:
    logger.error(f"Failed to configure Event Hub connection: {str(e)}")
    raise
try:
    cosmos_config, cosmos_format = ConfigManager.get_cosmos_config(env_scope, cosmos_endpoint, cosmos_sec_key, db_name, container_name)
except Exception as e:
    logger.error(f"Failed to configure Cosmos DB connection: {str(e)}")
    raise
try:
    df_eh = read_stream_data()
except Exception as e:
    logger.error(f"Failed to create Event Hub stream: {str(e)}")
    raise
try:
    streaming_query = write_stream_data_with_logging(df_eh)
    streaming_query.awaitTermination()
        
except Exception as e:
    logger.error(f"Failed to start streaming query: {str(e)}")
    raise