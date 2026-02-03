# Databricks notebook source
from datetime import datetime, timezone
import json

class ConfigManager:
    @staticmethod
    # Cosmos configuration method
    def get_cosmos_config(env_scope,cosmos_endpoint,cosmos_sec_key, db_name, container_name):
        
        cosmos_config = {
        "spark.cosmos.accountEndpoint": dbutils.secrets.get(scope=env_scope, key=cosmos_endpoint),  
        "spark.cosmos.accountKey": dbutils.secrets.get(scope=env_scope, key=cosmos_sec_key),
        "spark.cosmos.database": db_name,
        "spark.cosmos.container": container_name,
        "spark.cosmos.write.strategy": "ItemOverwrite",  
        "spark.cosmos.write.bulk.enabled": "true"
        }

        cosmos_format = 'cosmos.oltp'  
        return cosmos_config, cosmos_format
    
    # Eventhub configuration method

    def get_eventhub_config(
            ehscope,
            eventhub_name,
            eventhub_namespace,
            ehkey,
            eh_policy_name,
            maxoffset,
            consumer_group   # ✅ NEW: pass unique consumer group per job
        ):

        sharedaccesskey = dbutils.secrets.get(scope=ehscope, key=ehkey)

        connectionString = ("Endpoint=sb://" + eventhub_namespace + ".servicebus.windows.net/" + eventhub_name + ";EntityPath=" + eventhub_name + ";SharedAccessKeyName=" + eh_policy_name + ";SharedAccessKey=" + sharedaccesskey)

        # endTime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        startingEventPosition = {
            "offset": None,
            "seqNo": -1,
            "enqueuedTime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "isInclusive": False
        }

        ehConf = {}
        ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
        ehConf["eventhubs.maxEventsPerTrigger"] = maxoffset
        ehConf["eventhubs.consumerGroup"] = consumer_group
        ehConf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            connectionString
        )

        return ehConf

    
    # ADLS configuration method
    def get_adls_config(adls_scope, adls_key, storage_account):
        storage_key = dbutils.secrets.get(scope=adls_scope, key=adls_key)
        spark.conf.set(
            "fs.azure.account.key."+storage_account+".dfs.core.windows.net",
            storage_key)
        
    #Metadata call method
    def get_config(spark, module_name: str, table_name: str) -> dict:
        """
        Retrieve latest config as dictionary for a given module_name
        """
        df = spark.read.format("delta").load(table_name)
        row = (df.filter(F.col("module_name") == module_name.upper())
              .orderBy(F.col("insertion_datetime").desc())
              .limit(1)
              .collect())
        if not row:
            raise ValueError(f"No config found for module_name = {module_name}")
        return row[0].asDict()
        
    # KAFKA connector configuration method
    def get_kafka_connector_config(eh_scope,eh_namespace, eh_name, eh_key_name, eh_key_value, eh_consumer_group, maxoffset, startingoffsets):
        
        EH_CONN_SHARED_ACCESS_KEY_VALUE = dbutils.secrets.get(scope=eh_scope, key=eh_key_value)
        EH_CONN_STR = f"Endpoint=sb://{eh_namespace}.servicebus.windows.net/;SharedAccessKeyName={eh_key_name};SharedAccessKey={EH_CONN_SHARED_ACCESS_KEY_VALUE};EntityPath={eh_name}"
        KAFKA_OPTIONS = {
            'kafka.bootstrap.servers': f'{eh_namespace}.servicebus.windows.net:9093',
            'subscribe': eh_name,
            'kafka.sasl.mechanism': 'PLAIN',
            'kafka.security.protocol': 'SASL_SSL',
            'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";',
            "kafka.consumer.group.id": eh_consumer_group, 
            'kafka.request.timeout.ms': '60000',
            'kafka.session.timeout.ms': '30000',
            'maxOffsetsPerTrigger': maxoffset, 
            'failOnDataLoss': 'false',
            'startingOffsets': startingoffsets 
        }

        return KAFKA_OPTIONS

# COMMAND ----------

