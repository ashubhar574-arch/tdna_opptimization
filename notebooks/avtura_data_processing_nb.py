# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, trim, coalesce, lit, to_timestamp, date_format, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import *
import traceback
import uuid
from datetime import datetime, timezone
import json
import re
from dateutil import parser
import pytz
from pyspark.sql import Row

# Add these configurations for better performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true") 
spark.conf.set("spark.sql.adaptive.skew.enabled", "true")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.mapKeyDeduplicationPolicy", "LAST_WIN")

# ------------------------------------------------------------
# Friendly field mapping
# ------------------------------------------------------------
FRIENDLY_FIELD_MAP = {
    # arrival
    "DBD.dbd_start": "disembarking_start",
    "DBD.dbd_stop": "disembarking_finish",
    "RMP.disp_on_stand": "arrival_dispatcher_on_stand",
    "RMP.pbb_conn": "pbb_connected",
    "RMP.steps_conn": "placement_of_aircraft_steps_connected",
    "UNL.start": "ramp_unloading_start",

    # departure
    "DIS.ESBT_gate_open": "gate_open",
    "DRMP.disp_on_stand": "departure_dispatcher_on_stand",
    "DRMP.toilet_start": "toilet_service_start",
    "DRMP.toilet_stop": "toilet_service_finish",
    "DRMP.water_start": "potable_water_service_start",
    "DRMP.water_stop": "potable_water_service_finish",
    "DRMP.tug_arrived": "tug_connected",
    "DIS.ESBT_gate_closed": "gate_closed",
    "BRD.assist": "boarding_clearance",
    "BRD.start": "boarding_start",
    "BRD.stop": "boarding_finish",
    "BRD.total_pieces": "total_hand_baggage",
    "FUE.start": "fuelling_start",
    "FUE.stop": "fuelling_finish",
    "LDG.loading_start": "ramp_loading_start",
    "LDG.loading_stop": "ramp_loading_finish",
    "LDG.offload_recd": "offload_received",
    "DIS.eng_off": "engineer_off_finish",
    "DIS.cabin_door_closed": "cabin_doors_closed",
    "DRMP.cargo_positioned_on_stand": "cargo_positioned",
    "UNL.stop": "ramp_unloading_end",

    # dual events 
    "CLN.start": "cleaning_start",
    "CLN.stop": "cleaning_finish",
    "CTR.start": "catering_start",
    "CTR.stop": "catering_finish",

    # extra fields
    "linkedFlight":"linked_flight",
    "stand":"stand",
    "flightStatus":"flight_status",
    "flightStatusAD":"flight_status_ad",
    "flightStatusCode":"flight_status_code",
    "aircraftCallSign":"aircraft_call_sign",
    "et":"et",
    "gate":"gate",
    "Catering_Requested": "catering_requested",
    "Catering_Arrived": "catering_arrived",
    "delay_code1": "delay_code_1",
    "delay_duration_1": "delay_duration_1",
    "delay_code2": "delay_code_2",
    "delay_duration_2": "delay_duration_2",
    "delay_code3": "delay_code_3",
    "delay_duration_3": "delay_duration_3",
    "total_delay_duration": "total_delay_duration",
    "Departure - Temp on Arrival": "departure_temp_on_arrival",
    "Departure - Temp for Boarding": "departure_temp_for_boarding",
    "Departure - Boarding Held for Temp": "departure_boarding_held_for_temp",
    "Baggage Offloading Start": "baggage_offloading_start",
    "Baggage Offloading Finish": "baggage_offloading_finish",
    "DIS.cargo_door_closed": "cargo_door_closed"
}

# Cache the mapping lists for reuse
ARRIVAL_ORIGINAL_NAMES = [
    "DBD.dbd_start", "DBD.dbd_stop", "RMP.disp_on_stand",
    "RMP.pbb_conn", "RMP.steps_conn", "UNL.start"
]

DEPARTURE_ORIGINAL_NAMES = [
    "DIS.ESBT_gate_open", "DRMP.disp_on_stand", "DRMP.toilet_start",
    "DRMP.toilet_stop", "DRMP.water_start", "DRMP.water_stop",
    "DRMP.tug_arrived", "DIS.ESBT_gate_closed", "BRD.assist",
    "BRD.start", "BRD.stop", "BRD.total_pieces", "FUE.start",
    "FUE.stop", "LDG.loading_start", "LDG.loading_stop",
    "LDG.offload_recd", "DIS.eng_off", "DIS.cabin_door_closed",
    "DRMP.cargo_positioned_on_stand", "UNL.stop"
]

DUAL_EVENTS_ORIGINAL_NAMES = ["CLN.start", "CLN.stop", "CTR.start", "CTR.stop"]

ARRIVAL_EVENTS = [FRIENDLY_FIELD_MAP[name] for name in ARRIVAL_ORIGINAL_NAMES if name in FRIENDLY_FIELD_MAP]
DEPARTURE_EVENTS = [FRIENDLY_FIELD_MAP[name] for name in DEPARTURE_ORIGINAL_NAMES if name in FRIENDLY_FIELD_MAP]
DUAL_EVENTS = [FRIENDLY_FIELD_MAP[name] for name in DUAL_EVENTS_ORIGINAL_NAMES if name in FRIENDLY_FIELD_MAP]

standalone_fields_friendly = [
    "linked_flight","stand","flight_status","flight_status_ad","flight_status_code","aircraft_call_sign", "et", "gate", "catering_requested", "catering_arrived",
    "delay_code_1", "delay_duration_1", "delay_code_2", "delay_duration_2",
    "delay_code_3", "delay_duration_3", "total_delay_duration",
    "departure_temp_on_arrival", "departure_temp_for_boarding", 
    "departure_boarding_held_for_temp", "baggage_offloading_start", 
    "baggage_offloading_finish", "cargo_door_closed"
]

KEY_COLS = ["acdm_id", "flight_number", "scheduledate", "origin"]


# COMMAND ----------

def add_flight_key(df):
    """
    Enhanced flight key generation using scheduledate (date only) for partition key.
    """
    return (
        df.withColumn("carrier", F.regexp_extract(F.col("flight_number"), r"^([A-Za-z]+)", 1))
          .withColumn("flight_num_clean", F.regexp_replace(F.regexp_extract(F.col("flight_number"), r"(\d+)$", 1), r"^0+", ""))
          .withColumn("flight_key", F.concat_ws("-", 
                                              F.coalesce(F.col("carrier"), F.lit("no_carrier")),
                                              F.coalesce(F.col("flight_num_clean"), F.lit("no_fn")),
                                              F.coalesce(F.date_format(F.to_timestamp(F.col("scheduledate")), "yyyy-MM-dd"), F.lit("no_date")),
                                              F.coalesce(F.col("origin").cast("string"), F.lit("no_orig"))))
          .withColumn("flight_number", F.col("flight_num_clean"))
          .withColumn("flight_type", F.upper(F.col("flight_type")))
          .withColumn("airline", F.col("carrier"))
          .drop("flight_num_clean", "carrier")  
    )

# COMMAND ----------

def normalize_timestamp_format(timestamp_str):
    """
    Improved timestamp normalization with better error handling.
    """
    if timestamp_str is None:
        return None
    
    # Convert to string
    str_value = str(timestamp_str).strip()
    
    # Handle empty strings
    if not str_value or str_value.lower() == 'null':
        return None
    
    try:
        float(str_value)
        return str_value
    except:
        pass
    
    try:
        if str_value.endswith('Z') and 'T' in str_value:
            parser.isoparse(str_value)
            return str_value
        
        dt = parser.isoparse(str_value)
        
        if dt.tzinfo is not None:
            dt_utc = dt.astimezone(timezone.utc)
        else:
            dt_utc = dt.replace(tzinfo=timezone.utc)
        
        normalized = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        return normalized
    except:
        return str_value
    
    except Exception as e:
        print(f"Timestamp normalization failed for '{timestamp_str}': {str(e)}")
        return None
    
normalize_timestamp_udf = F.udf(normalize_timestamp_format, StringType())    

# COMMAND ----------

# ------------------------------------------------------------
# Schedule date validation function
# ------------------------------------------------------------
def validate_schedule_date(schedule_date_str):
    """
    Validate schedule date format and return normalized date or None if invalid
    """
    if not schedule_date_str:
        return None
        
    try:
        schedule_date_str = str(schedule_date_str).strip()
        
        normalized_ts = normalize_timestamp_format(schedule_date_str)
        if not normalized_ts:
            return None
            
        date_part = normalized_ts.split('T')[0]
        
        datetime.strptime(date_part, "%Y-%m-%d")
        
        return date_part
        
    except Exception as e:
        print(f"Schedule date validation failed for '{schedule_date_str}': {str(e)}")
        return None

validate_schedule_date_udf = F.udf(validate_schedule_date, StringType())

# COMMAND ----------

def write_raw_data_to_delta(batch_df, batch_id):
    """
    Function to write raw data to ADLS in Delta format
    """
    start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    
    try:
        (batch_df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .option("optimizeWrite", "true")
         .save(raw_dir))
        
    except Exception as e:
        print(f"Error writing to raw data: {str(e)}")  
        traceback.print_exc()
        return  

    try:        
        parsed_for_product = batch_df.withColumn("parsed_json", from_json(F.col("json_body"), avtura_schema))
        
        base_df = parsed_for_product.select(
            F.col("parsed_json.flightNumber").alias("flight_number"),
            F.date_format(F.to_timestamp(F.col("parsed_json.scheduleDate")), "yyyy-MM-dd").alias("scheduledate"),
            F.col("parsed_json.origin").alias("origin"),
            F.upper(F.col("parsed_json.flightType")).alias("flight_type"),
            F.col("enqueuedTime").alias("enqueuedTime")
        ).filter(
            F.col("flight_number").isNotNull() & 
            F.col("scheduledate").isNotNull() & 
            F.col("origin").isNotNull()
        )
        
        batch_df_with_key = add_flight_key(base_df)

        product_select_expr = [            
            F.col('flight_key').alias('flight_key'), 
            F.col('enqueuedTime').alias('enqueuedTime'),
            current_timestamp().alias('ingestion_datetime'),
            current_timestamp().alias('cosmos_process_datetime'),
            current_date().alias("ingest_date")
        ]

        df_with_extracted_product = batch_df_with_key.select(product_select_expr)
        
        # Save product data with optimization
        (df_with_extracted_product.write
            .format("delta")
            .option("mergeSchema", "true")
            .option("optimizeWrite", "true")
            .mode("append")
            .partitionBy("ingest_date")
            .save(product_dir))    

        end_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        duration = (end_time - start_time).total_seconds()
        
    except Exception as e:
        end_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        error_msg = f"Failed to process product data: {str(e)}"
        print(f"{error_msg}")
        traceback.print_exc()
        raise

# COMMAND ----------

def is_valid_json(json_str):
    """
    Validate if string is valid JSON
    """
    try:
        if json_str is None:
            return False
        json.loads(json_str)
        return True
    except (json.JSONDecodeError, TypeError):
        return False

is_valid_json_udf = F.udf(is_valid_json, BooleanType())

def _create_friendly_mapping_expr():
    """Create friendly mapping expression once for reuse"""
    friendly_conditions = []
    for orig, friendly in FRIENDLY_FIELD_MAP.items():
        friendly_conditions.append((F.col("original_name") == orig, lit(friendly)))
    
    friendly_expr = F
    for condition, result in friendly_conditions:
        friendly_expr = friendly_expr.when(condition, result)
    friendly_expr = friendly_expr.otherwise(lit(None))
    return friendly_expr

friendly_mapping_expr = _create_friendly_mapping_expr()

# COMMAND ----------

def process_avtura_batch_data(batch_df, batch_id):
    """
    OPTIMIZED Main function to process Avtura batch data
    """
    start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    total_raw = batch_df.count()
    
    print(f"Processing batch {batch_id} with {total_raw} records...")
    
    try:
        write_raw_data_to_delta(batch_df, batch_id)
        parsed_df = batch_df.withColumn("parsed_json", F.when(is_valid_json_udf(F.col("json_body")),from_json(F.col("json_body"), avtura_schema))).filter(F.col("parsed_json").isNotNull())

        base_df = parsed_df.select(
            # Key fields
            F.col("parsed_json.acdmId").alias("acdm_id"),
            F.col("parsed_json.flightNumber").alias("flight_number"),
            F.col("parsed_json.scheduleDate").alias("scheduledate"),
            F.col("parsed_json.origin").alias("origin"),
            
            # Base fields
            F.regexp_replace(F.col("parsed_json.aircraftReg"), "-", "").alias("aircraft_reg"),
            F.col("parsed_json.aircraftType").alias("aircraft_type"),
            F.col("parsed_json.airline").alias("airline_src"),
            F.col("parsed_json.at").alias("at"),
            F.col("parsed_json.cancelled").alias("cancelled"),
            F.col("parsed_json.deleted").alias("deleted"),
            F.col("parsed_json.destination").alias("destination"),
            F.col("parsed_json.diverted").alias("diverted"),
            F.upper(F.col("parsed_json.flightType")).alias("flight_type"),
            F.col("parsed_json.partial").alias("partial"),
            F.col("parsed_json.source").alias("source"),
            F.col("parsed_json.sourceLocation").alias("source_location"),
            F.col("parsed_json.st").alias("st"),
            
            # Standalone fields
            F.col("parsed_json.linkedFlight").alias("linked_flight"),
            F.col("parsed_json.stand").alias("stand"),
            F.col("parsed_json.flightStatus").alias("flight_status"),
            F.col("parsed_json.flightStatusAD").alias("flight_status_ad"),
            F.col("parsed_json.flightStatusCode").alias("flight_status_code"),
            F.col("parsed_json.aircraftCallSign").alias("aircraft_call_sign"),
            F.col("parsed_json.et").alias("et"),
            F.col("parsed_json.gate").alias("gate"),
            F.col("parsed_json.Catering_Requested").alias("catering_requested"),
            F.col("parsed_json.Catering_Arrived").alias("catering_arrived"),
            F.col("parsed_json.delay_code1").alias("delay_code_1"),
            F.col("parsed_json.delay_duration_1").alias("delay_duration_1"),
            F.col("parsed_json.delay_code2").alias("delay_code_2"),
            F.col("parsed_json.delay_duration_2").alias("delay_duration_2"),
            F.col("parsed_json.delay_code3").alias("delay_code_3"),
            F.col("parsed_json.delay_duration_3").alias("delay_duration_3"),
            F.col("parsed_json.total_delay_duration").alias("total_delay_duration"),
            F.col("parsed_json.Departure - Temp on Arrival").alias("departure_temp_on_arrival"),
            F.col("parsed_json.Departure - Temp for Boarding").alias("departure_temp_for_boarding"),
            F.col("parsed_json.Departure - Boarding Held for Temp").alias("departure_boarding_held_for_temp"),
            F.col("parsed_json.Baggage Offloading Start").alias("baggage_offloading_start"),
            F.col("parsed_json.Baggage Offloading Finish").alias("baggage_offloading_finish"),
            
            # Events array
            F.col("parsed_json.flightDataUpdateItem").alias("flight_data_update_item"),
            
            # Metadata
            F.col("enqueuedTime").alias("enqueued_datetime")
        ).filter(
            F.col("acdm_id").isNotNull() & 
            F.col("flight_number").isNotNull() & 
            F.col("scheduledate").isNotNull()
        )
        window = Window.partitionBy("flight_number", "scheduledate", "origin").orderBy(F.col("enqueued_datetime").desc())
        base_df = base_df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

        base_df = base_df.withColumn(
            "validated_scheduledate", 
            validate_schedule_date_udf(F.col("scheduledate"))
        )
        
        # Filter valid schedule dates
        base_df = base_df.filter(F.col("validated_scheduledate").isNotNull())
        events_map_df = base_df.select(
            *KEY_COLS, "flight_type", "flight_data_update_item"
        ).select(
            *KEY_COLS, "flight_type",
            F.explode_outer("flight_data_update_item").alias("event")
        ).select(
            *KEY_COLS, "flight_type",
            F.col("event.name").alias("original_name"),
            F.col("event.value.value").alias("event_value")
        ).filter(F.col("original_name").isNotNull()) \
         .withColumn("friendly_name", friendly_mapping_expr) \
         .filter(F.col("friendly_name").isNotNull()) \
         .withColumn("normalized_value", normalize_timestamp_udf(F.col("event_value"))) \
         .groupBy(*KEY_COLS).agg(
             F.map_from_entries(
                 F.collect_list(
                     F.struct("friendly_name", "normalized_value")
                 )
             ).alias("events_map")
         )

        # Single join and transformation chain
        enriched_df = base_df.join(events_map_df, KEY_COLS, "left") \
            .withColumn("events_map", F.coalesce(F.col("events_map"), F.create_map().cast("map<string,string>"))) \
            .withColumn("cargo_door_closed", F.col("events_map").getItem("cargo_door_closed"))

        # Add arrival_pax_off keys
        enriched_df = enriched_df.withColumn(
            "events_map",
            F.map_concat(
                F.col("events_map"),
                F.create_map(
                    lit("arrival_pax_off_start"), F.col("events_map").getItem("disembarking_start"),
                    lit("arrival_pax_off_finish"), F.col("events_map").getItem("disembarking_finish")
                )
            )
        )

        # Pre-define map creation functions
        def create_map_expr(keys_list):
            """Create map expression for given keys"""
            map_parts = []
            for key in keys_list:
                map_parts.extend([lit(key), F.col("events_map").getItem(key)])
            return F.create_map(*map_parts)

        # Create arrival/departure maps based on flight type
        arrival_keys_arrival = ARRIVAL_EVENTS + ["arrival_pax_off_start", "arrival_pax_off_finish"] + DUAL_EVENTS
        departure_keys_departure = DEPARTURE_EVENTS + DUAL_EVENTS
        
        arrival_map = F.when(
            F.col("flight_type") == "ARRIVAL",
            create_map_expr(arrival_keys_arrival)
        ).otherwise(
            create_map_expr(ARRIVAL_EVENTS + ["arrival_pax_off_start", "arrival_pax_off_finish"])
        )
        
        departure_map = F.when(
            F.col("flight_type") == "DEPARTURE",
            create_map_expr(departure_keys_departure)
        ).otherwise(
            create_map_expr(DEPARTURE_EVENTS)
        )

        # Final transformations in single chain
        final_df = enriched_df \
            .withColumn("arrival", F.array(arrival_map)) \
            .withColumn("departure", F.array(departure_map)) \
            .withColumn("st", normalize_timestamp_udf(F.col("st"))) \
            .withColumn("at", normalize_timestamp_udf(F.col("at"))) \
            .withColumn("schedule_datetime", normalize_timestamp_udf(F.col("scheduledate"))) \
            .withColumn("scheduledate", F.col("validated_scheduledate")) \
            .drop("validated_scheduledate", "flight_data_update_item", "events_map")

        # Apply the flight key function
        flight_key_df = add_flight_key(final_df)    

        # Apply flight key and final columns
        cosmos_df = flight_key_df \
            .withColumn("id", F.col("flight_key")) \
            .withColumn("ingestion_datetime", normalize_timestamp_udf(F.current_timestamp().cast("string"))) \
            .withColumn("cosmos_date", date_format(current_timestamp(), "yyyy-MM-dd")) \
            .withColumn("cosmos_process_datetime", normalize_timestamp_udf(F.current_timestamp().cast("string"))) \
            .withColumn("enqueued_datetime", normalize_timestamp_udf(F.col("enqueued_datetime")))\
            .select(
                "id", "flight_key", "acdm_id", "aircraft_reg", "aircraft_type", "airline", "airline_src",
                "at", "cancelled", "deleted", "destination", "diverted",
                "flight_number", "flight_type", "origin", "partial", 
                "scheduledate", "schedule_datetime",
                "source", "source_location", "st",
                "arrival", "departure", "linked_flight",
                "stand", "flight_status", "flight_status_ad", "flight_status_code", 
                "aircraft_call_sign", "et", "gate", "catering_requested", "catering_arrived",
                "delay_code_1", "delay_duration_1", "delay_code_2", "delay_duration_2",
                "delay_code_3", "delay_duration_3", "total_delay_duration",
                "departure_temp_on_arrival", "departure_temp_for_boarding", 
                "departure_boarding_held_for_temp", "baggage_offloading_start", 
                "baggage_offloading_finish", "cargo_door_closed", "enqueued_datetime",
                "ingestion_datetime", "cosmos_date", "cosmos_process_datetime"
            )

        final_count = cosmos_df.count()
        
        # STEP 7: Write to Cosmos DB
        if final_count > 0:
            
            # Get Cosmos configuration
            cosmos_config, cosmos_format = ConfigManager.get_cosmos_config(env_scope, cosmos_endpoint, cosmos_sec_key, db_name, container_name)
            
            # Write with optimized settings
            (cosmos_df.write
                .format("cosmos.oltp")
                .mode("append")
                .option("spark.cosmos.write.bulk.enabled", "true")
                .option("spark.cosmos.write.bulk.maxPendingOperations", 100000)
                .option("spark.cosmos.write.strategy", "ItemOverwrite")
                .option("spark.cosmos.write.maxRetryCount", 3)
                .options(**cosmos_config)
                .save())
            
            end_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            duration = (end_time - start_time).total_seconds()
            
            print(f"SUCCESS: Wrote {final_count} records to Cosmos DB")
            return cosmos_df
        else:
            print(f"No records to write to Cosmos DB")
            return None

    except Exception as e:
        error_msg = f"ETL process failed: {str(e)}"
        print(f" {error_msg}")
        traceback.print_exc()
        return None

# COMMAND ----------

def read_stream_data():
    df_eh = (spark.readStream
          .format("eventhubs")
          .options(**ehConf)
          .load()
          .withColumn("json_body", expr("CAST(Body as string)"))
          .withColumn("ingestiontime", current_timestamp())
          .withColumn("time_difference_seconds", expr("unix_timestamp(ingestiontime) - unix_timestamp(enqueuedTime)"))
          .select("json_body", "ingestiontime", "enqueuedTime", "time_difference_seconds", "sequenceNumber"))
    return df_eh

# COMMAND ----------

def process_batch(batch_df, batch_id):
    """
    Process each micro-batch in the stream
    """
    start_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    
    try:        
        # Process the batch
        result = process_avtura_batch_data(batch_df, batch_id)
        
        end_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        duration = (end_time - start_time).total_seconds()
        
        if result is not None:
            processed_count = result.count() if hasattr(result, 'count') else 0
        else:
            print(f"Batch {batch_id} completed with no results in {duration:.2f}s")
            
        return result

    except Exception as e:
        end_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        duration = (end_time - start_time).total_seconds()
        print(f"Batch {batch_id} failed after {duration:.2f}s: {str(e)}")
        raise

def write_stream_data_with_logging(df_eh):
    """
    Enhanced streaming write function
    """
    streaming_query = (df_eh.writeStream
        .queryName("avtura_stream_processing")
        .option("checkpointLocation", deltaFolderCheckpoint)
        .trigger(processingTime='10 seconds')  
        .foreachBatch(process_batch)
        .start())
    return streaming_query