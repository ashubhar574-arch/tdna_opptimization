from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType, TimestampType, DoubleType, LongType
from pyspark.sql.functions import expr, current_timestamp
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, from_json, get_json_object, lit, expr
from pyspark.sql.functions import expr, current_timestamp

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


####################################################################################

reference_raw_path = "/Volumes/ops_catalog/cxops/edpppecops_raw/eag/ey/guest_preference_reference_zone/"
preference_master_df_upsert = "/Volumes/ops_catalog/cxops/edpppecops_product/eag/ey/preference_master_df_upsert/"
preference_master_df_append = "/Volumes/ops_catalog/cxops/edpppecops_product/eag/ey/preference_master_df_append/"
tdna_membership_path = "/Volumes/ops_catalog/cxops/edpppecops_product/eag/ey/TDNAMemberships_dummy/"

reference_df = (
    spark.read
    .format("json")
    .option("multiline", "true")
    .option("recursiveFileLookup", "true")
    .load(reference_raw_path)
)

reference_df = (
    reference_df
    .select(
        col("preferences.ffpNumber").alias("ffp_number"),

        F.array(
            F.struct(
                col("preferences.ffpNumber").alias("ffpNumber"),
                col("preferences.personalInformation").alias("personalInformation"),
                col("preferences.diningPreferences").alias("diningPreferences"),
                col("preferences.productAndServiceDelivery").alias("productAndServiceDelivery"),
                col("preferences.expectations").alias("expectations")
            )
        ).alias("preference_body"),

        F.current_timestamp().alias("ingestion_datetime"),
        F.current_date().alias("ingestion_date")
    )
    .filter(col("ffp_number").isNotNull())
)

read_from_tdna_df = (
    spark.read
    .format("delta")
    .load(tdna_membership_path)
    .select(
        col("profile_id"),
        col("memberId").alias("ffp_number")))
    
preferences_with_profile = (
    broadcast(reference_df)
    .join(
        read_from_tdna_df,
        on="ffp_number",
        how="left"
    )
    .select(
        "profile_id",
        "ffp_number",
        "preference_body",
        "ingestion_datetime",
        "ingestion_date"
    )
    .distinct()
)

print(f"Preferences joined with profiles: {preferences_with_profile.count()} records")

preferences_with_profile.write.format("delta").mode("append").save(preference_master_df_append)   
print(f"Appended records to {preference_master_df_append}")

if DeltaTable.isDeltaTable(spark, preference_master_df_upsert):

    target_table = DeltaTable.forPath(spark, preference_master_df_upsert)

    target_table.alias("target").merge(
        preferences_with_profile.alias("source"),
        "target.profile_id = source.profile_id AND target.ffp_number = source.ffp_number"
    ).whenMatchedUpdate(
        condition="target.preference_body != source.preference_body",
        set={
            "preference_body": "source.preference_body",
            "ingestion_datetime": "source.ingestion_datetime",
            "ingestion_date": "source.ingestion_date"
        }
    ).whenNotMatchedInsertAll().execute()

    print(f"Preferences data successfully merged into {preference_master_df_upsert}")

else:
    preferences_with_profile.write.format("delta").mode("overwrite").save(preference_master_df_upsert)
    print(f"Master preferences table created at {preference_master_df_upsert}")
