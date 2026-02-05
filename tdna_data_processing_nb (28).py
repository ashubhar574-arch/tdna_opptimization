# Databricks notebook source
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

# COMMAND ----------

def cem_like(col_id):
    return F.lower(col_id).rlike(r"^cem_derived_data(-\d+)?$")

# COMMAND ----------

def extract_data_with_flattened_sources(df, array_path, source_path, parent_cols, value_exprs, data_type, document_filter_condition=None, batch_id=None, processing_stage=""):
    max_retries = 2
    for attempt in range(max_retries):
        try:
            exploded_df = (
                df
                .withColumn("item", explode_outer(col(array_path))) 
                .withColumn("src", explode_outer(expr(source_path))) 
                .filter(cem_like(col("src.id"))) 
            )
            
            data_df = exploded_df.select(
                *parent_cols,
                struct(
                    *value_exprs,
                    col("src.id").alias("source_id"),
                    col("src.sourceType").alias("source_type")
                ).alias("data")       
            ).distinct()
            
            
            result_df = (
                data_df
                .groupBy(*parent_cols)
                .agg(F.collect_set("data").alias(f"{data_type}")) #source details are combined with each of detail
            )
            
            return result_df
            
        except Exception as e:
            if ("interrupted" in str(e).lower() or "cancel" in str(e).lower()) and attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
                continue
            else:
                print(f"[{batch_id}] {processing_stage}: Error occurred, returning empty result")
                return df.select(*parent_cols).limit(0).withColumn(f"{data_type}", F.array().cast(ArrayType(StructType())))


# COMMAND ----------

# def process_tdna_data(parsed_df, batch_id=None, bridge_table_path=None):
#     max_retries = 3
#     for attempt in range(max_retries):
#         try:
#             df_with_context = parsed_df.selectExpr(
#                 "parsed_json.LiveFeed.customer.profile.id as profile_id",
#                 "parsed_json.LiveFeed.customer.profile.version as profile_version",
#                 "parsed_json.LiveFeed.context.timestamp as timestamp",
#                 "parsed_json.LiveFeed.context.process as context_process",
#                 f"date_format(to_timestamp(enqueuedTime), 'yyyy-MM-dd\\'T\\'HH:mm:ss\\'Z\\'') as enqueued_datetime",
#                 "parsed_json"
#             )

#             w_latest = Window.partitionBy("profile_id").orderBy(expr("cast(profile_version as double) desc"))
#             latest_df = df_with_context.withColumn("rn", row_number().over(w_latest)).filter(col("rn") == 1).drop("rn")

#             tdna_df = latest_df.selectExpr(
#                 "profile_id",
#                 "profile_version",
#                 "timestamp as context_datetime",
#                 "context_process",
#                 "enqueued_datetime",
#                 "parsed_json.LiveFeed.customer.profile.identity.names as name",
#                 "parsed_json.LiveFeed.customer.profile.identity.documents as identity_documents",
#                 "parsed_json.LiveFeed.customer.profile.identity.demographics as demographics",
#                 "parsed_json.LiveFeed.customer.profile.contact.emails as emails",
#                 "parsed_json.LiveFeed.customer.profile.contact.phones as phones",
#                 "parsed_json.LiveFeed.customer.profile.contact.addresses as addresses",
#                 "parsed_json.LiveFeed.customer.profile.links as links",
#                 "parsed_json.LiveFeed.customer.profile.metrics as metrics",
#                 "parsed_json.LiveFeed.customer.profile.memberships as memberships",
#                 "parsed_json.LiveFeed.dictionary.pnrSources as pnr_sources",
#                 "parsed_json.LiveFeed.dictionary.ticketingSources as ticketing_sources"
#             )

#             doc_result = extract_data_with_flattened_sources(
#                 tdna_df, "identity_documents", "item.sources",
#                 ["profile_id", "profile_version"],
#                 [
#                     col("item.documentType").alias("document_type"),
#                     col("item.number").alias("number"),
#                     col("item.gender").alias("gender"),
#                     col("item.issuanceCountry").alias("issuance_country"),
#                     col("item.issuanceDate").alias("issuance_date"),
#                     col("item.issuanceLocation").alias("issuance_location"),
#                     col("item.expiryDate").alias("expiry_date"),
#                     col("item.birthDate").alias("birth_date"),
#                     col("item.birthCountry").alias("birth_country"),
#                     col("item.birthPlace").alias("birth_place"),
#                     col("item.civilStatus").alias("civil_status"),
#                     col("item.label").alias("label"),
#                     col("item.nationality").alias("nationality"),
#                     col("item.effectiveDate").alias("effective_date"),
#                     col("item.rank").alias("rank"),
#                     col("item.remarks").alias("remarks")
#                 ], "documents", None, batch_id, "Documents Processing"
#             )

#             doc_result = doc_result.withColumn(
#                 "documents",
#                 expr("""
#                     filter(documents, x -> 
#                         lower(x.document_type) in ('passport', 'visa') OR
#                         x.document_type in ('PASSPORT', 'VISA')
#                     )
#                 """)
#             )

#             name_result = extract_data_with_flattened_sources(
#                 tdna_df, "name", "item.universal.sources",
#                 ["profile_id", "profile_version"],
#                 [
#                     col("item.universal.firstName").alias("first_name"),
#                     col("item.universal.lastName").alias("last_name"),
#                     expr("coalesce(trim(element_at(item.universal.titles, 1)), '')").alias("title"),
#                     expr("coalesce(trim(element_at(item.universal.middleNames, 1)), '')").alias("middle_name")
#                 ], "name", None, batch_id, "Names Processing"
#             )

#             demographics_result = extract_data_with_flattened_sources(
#                 tdna_df, "demographics", "item.sources",
#                 ["profile_id", "profile_version"],
#                 [
#                     col("item.birthDate").alias("birth_date"),
#                     col("item.birthCountry").alias("birth_country"),
#                     col("item.birthPlace").alias("birth_place"),
#                     col("item.nationalities").alias("nationalities"),
#                     col("item.gender").alias("gender"),
#                     col("item.civilStatus").alias("civil_status")
#                 ], "demographics", None, batch_id, "Demographics Processing"
#             )

#             email_result = extract_data_with_flattened_sources(
#                 tdna_df, "emails", "item.sources",
#                 ["profile_id", "profile_version"],
#                 [col("item.address").alias("email_address")], "emails", None, batch_id, "Emails Processing"
#             )

#             phone_result = extract_data_with_flattened_sources(
#                 tdna_df, "phones", "item.sources",
#                 ["profile_id", "profile_version"],
#                 [col("item.number").alias("phone_number")], "phones", None, batch_id, "Phones Processing"
#             )
#             address_result = extract_data_with_flattened_sources(
#                 tdna_df, "addresses", "item.sources",
#                 ["profile_id", "profile_version"],
#                 [
#                     col("item.cityName").alias("city"),
#                     col("item.stateName").alias("state"),
#                     col("item.countryName").alias("country"),
#                     col("item.countryCode").alias("country_code"),
#                     col("item.stateCode").alias("state_code"),
#                     col("item.postalCode").alias("postal_code"),
#                     col("item.postalBox").alias("postal_box")
#                 ], "addresses", None, batch_id, "Addresses Processing"
#             )

#             metric_result = tdna_df.select(
#                 "profile_id",
#                 "profile_version",
#                 F.explode_outer("metrics").alias("metric_item")
#             ).select(
#                 "profile_id",
#                 "profile_version",
#                 F.explode_outer("metric_item.sources").alias("source"),
#                 col("metric_item.code").alias("code"),
#                 col("metric_item.name").alias("name"),
#                 col("metric_item.value").alias("value")
#             ).select(
#                 "profile_id",
#                 "profile_version",
#                 col("source.id").alias("source_id"),
#                 col("source.sourceType").alias("source_type"),
#                 "code",
#                 "name",
#                 "value"
#             ).filter((col("code").isNotNull()) | (col("name").isNotNull()) |(col("value").isNotNull()) | (col("source_id").isNotNull())).distinct().groupBy("profile_id", "profile_version").agg(
#                 F.collect_list(
#                     F.struct(
#                         col("code").alias("code"),
#                         col("name").alias("name"),
#                         col("value").alias("value"),
#                         col("source_id").alias("source_id"),
#                         col("source_type").alias("source_type")
#                     )
#                 ).alias("metrics")
#             )
#             all_profiles = tdna_df.select("profile_id", "profile_version").distinct()
#             metric_result = all_profiles.join(metric_result,["profile_id", "profile_version"],"left").withColumn("metrics",F.when(F.col("metrics").isNull(), F.array()).otherwise(F.col("metrics")))

#             membership_result = extract_data_with_flattened_sources(
#                 tdna_df, "memberships", "item.sources",
#                 ["profile_id", "profile_version"],
#                 [
#                     col("item.membershipType").alias("membership_type"),
#                     col("item.memberId").alias("member_id"),
#                     col("item.rank").alias("membership_rank"),
#                     col("item.program.owner").alias("program_owner"),
#                     col("item.activeTier.name").alias("active_tier_name"),
#                     col("item.activeTier.code").alias("active_tier_code"),
#                     col("item.activeTier.priorityCode").alias("active_tier_priority"),
#                     col("item.allianceTier.name").alias("alliance_tier_name"),
#                     col("item.allianceTier.code").alias("alliance_tier_code"),
#                     col("item.allianceTier.priorityCode").alias("alliance_tier_priority")
#                 ], "memberships", None, batch_id, "Memberships Processing"
#             )

#             try:
#                 # Combine explosions where possible
#                 link_exploded = tdna_df.selectExpr(
#                     "profile_id",
#                     "profile_version",
#                     "explode_outer(links) as link_item"
#                 ).selectExpr(
#                     "profile_id",
#                     "profile_version",
#                     "explode_outer(link_item.sources) as link_source",
#                     "link_item.link.pnr.id as pnr_link_id",
#                     "link_item.link.ticketing.ticket.number as ticketing_link_ticket_number",
#                     "link_item.link.pnr.booking.locator.internal.value as link_pnr_internal_locator",
#                     "link_item.link.pnr.booking.locator.external.value as link_pnr_external_locator"
#                 ).selectExpr(
#                     "profile_id",
#                     "profile_version",
#                     "pnr_link_id",
#                     "ticketing_link_ticket_number",
#                     "link_pnr_internal_locator",
#                     "link_pnr_external_locator",
#                     "link_source.id as link_source_id",
#                     "link_source.sourceType as link_source_type"
#                 )

#                 # Optimized PNR extraction
#                 pnr_exploded = tdna_df.selectExpr(
#                     "profile_id",
#                     "profile_version",
#                     "explode_outer(pnr_sources) as pnr_item"
#                 ).selectExpr(
#                     "profile_id",
#                     "profile_version",
#                     "pnr_item.id as pnr_id",
#                     "pnr_item.booking.createdAt as pnr_creation_datetime",
#                     "pnr_item.booking.lastModifiedAt as pnr_last_modified_datetime",
#                     "pnr_item.booking.numberInParty as pnr_number_in_party",
#                     "pnr_item.passengerTattoo as pnr_passenger_tattoo",
#                     "pnr_item.booking.locator.internal.value as pnr_internal_locator",
#                     "pnr_item.booking.locator.external.value as pnr_external_locator"
#                 ).distinct()

#                 # Optimized ticketing extraction
#                 ticketing_exploded = tdna_df.selectExpr(
#                     "profile_id",
#                     "profile_version",
#                     "explode_outer(ticketing_sources) as ticketing_item"
#                 ).selectExpr(
#                     "profile_id",
#                     "profile_version",
#                     "ticketing_item.id as ticketing_id",
#                     "ticketing_item.lastModifiedAt as ticketing_last_modified_datetime",
#                     "ticketing_item.receivedAt as ticketing_received_datetime",
#                     "ticketing_item.ticket.number as ticketing_ticket_number"
#                 ).distinct()

#                 all_pnrs_with_links = pnr_exploded.alias("p").join(
#                     link_exploded.alias("l"),
#                     expr("""
#                         p.profile_id = l.profile_id AND 
#                         p.profile_version = l.profile_version AND 
#                         (p.pnr_id = l.link_source_id OR p.pnr_id = l.pnr_link_id)
#                     """),
#                     "left"
#                 ).selectExpr(
#                     "p.profile_id",
#                     "p.profile_version",
#                     "p.pnr_id",
#                     "p.pnr_creation_datetime",
#                     "p.pnr_last_modified_datetime",
#                     "p.pnr_number_in_party",
#                     "p.pnr_passenger_tattoo",
#                     "p.pnr_internal_locator",
#                     "p.pnr_external_locator",
#                     "l.ticketing_link_ticket_number"
#                 ).distinct()

#                 final_combined_matches = all_pnrs_with_links.alias("a").join(
#                     ticketing_exploded.alias("t"),
#                     expr("""
#                         a.profile_id = t.profile_id AND 
#                         a.profile_version = t.profile_version AND 
#                         a.ticketing_link_ticket_number = t.ticketing_ticket_number
#                     """),
#                     "left"
#                 ).selectExpr(
#                     "a.profile_id",
#                     "a.profile_version",
#                     "a.pnr_id",
#                     "a.pnr_creation_datetime",
#                     "a.pnr_last_modified_datetime",
#                     "a.pnr_number_in_party",
#                     "a.pnr_passenger_tattoo",
#                     "a.pnr_internal_locator",
#                     "a.pnr_external_locator",
#                     "t.ticketing_id",
#                     "t.ticketing_last_modified_datetime",
#                     "t.ticketing_received_datetime",
#                     "t.ticketing_ticket_number"
#                 ).distinct()

#                 # Step 6: Create traveler_id
#                 combined_with_traveler_id = final_combined_matches.withColumn(
#                     "pnr_number", 
#                     coalesce(col("pnr_internal_locator"), col("pnr_external_locator"))
#                 ).withColumn(
#                     "traveler_id",
#                     when(
#                         col("pnr_number").isNotNull() & 
#                         col("pnr_creation_datetime").isNotNull() & 
#                         col("pnr_passenger_tattoo").isNotNull(),
#                         concat_ws(
#                             "-",
#                             col("pnr_number"),
#                             date_format(to_date(col("pnr_creation_datetime")), "yyyy-MM-dd"),
#                             concat_ws("-", lit("PT"), col("pnr_passenger_tattoo"))
#                         )
#                     ).otherwise(lit(None))
#                 )
                
#                 # # Step 7: Read bridge table and join to get dcs_id
#                 # dcs_id_column = lit(None).cast("string").alias("dcs_id")
                
#                 # if bridge_table_path:
#                 #     try:
#                 #         # Read the bridge table
#                 #         bridge_df = spark.read.format("delta").load("/Volumes/ops_catalog/cxops/edpppecops_product/guest_hub/bridge_table/")
                        
#                 #         # Join with bridge table on traveler_id
#                 #         combined_with_dcs = combined_with_traveler_id.join(
#                 #             bridge_df.select("traveler_id", "dcs_id").distinct(),
#                 #             on="traveler_id",
#                 #             how="left"
#                 #         )
#                 #     except Exception as bridge_err:
#                 #         print(f" [{batch_id}] Warning: Could not read bridge table: {str(bridge_err)}")
#                 #         # If bridge table can't be read, add null dcs_id column
#                 #         combined_with_dcs = combined_with_traveler_id.withColumn("dcs_id", lit(None))
#                 # else:
#                 #     # If no bridge table path provided, add null dcs_id column
#                 #     combined_with_dcs = combined_with_traveler_id.withColumn("dcs_id", lit(None))

#                 # Step 8: Create final dictionary structure with dcs_id
#                 combined_dict_df = combined_with_traveler_id.select(
#                     "profile_id", 
#                     "profile_version",
#                     struct(
#                         col("ticketing_last_modified_datetime").alias("ticketing_last_modified_datetime"),
#                         col("ticketing_received_datetime").alias("ticketing_received_datetime"),
#                         col("ticketing_id").alias("ticket_id"),
#                         col("ticketing_ticket_number").alias("ticket_number"),
#                         col("pnr_creation_datetime").alias("pnr_creation_datetime"),
#                         col("pnr_last_modified_datetime").alias("pnr_last_modified_datetime"),
#                         col("pnr_id").alias("pnr_link_id"),
#                         col("pnr_number_in_party").alias("number_in_party"),
#                         col("pnr_passenger_tattoo").alias("passenger_tattoo"),
#                         col("pnr_number").alias("pnr_number"),
#                         col("traveler_id").alias("traveler_ids")
#                         #col("dcs_id").alias("dcs_id")  
#                     ).alias("data")
#                 ).distinct().groupBy("profile_id", "profile_version").agg(
#                     F.collect_set("data").alias("dictionary")
#                 )
   
#             except Exception as e:
#                 print(f" [{batch_id}] Error processing dictionary data: {str(e)}")
#                 traceback.print_exc()
#                 combined_dict_df = tdna_df.select("profile_id", "profile_version").distinct().withColumn(
#                     "dictionary", F.array().cast(ArrayType(StructType()))
#                 )
#                 print(f" [{batch_id}] Empty dictionary created: {combined_dict_df.count()} records")

#             # Optimized joins by starting with all results and reducing joins
#             all_results = [name_result, doc_result, demographics_result, email_result,
#                           phone_result, address_result, metric_result, membership_result, combined_dict_df]

#             # Start with base and sequentially join
#             base_profiles = tdna_df.select("profile_id", "profile_version").distinct()
#             combined_df = base_profiles
#             for result_df in all_results:
#                 combined_df = combined_df.join(result_df, ["profile_id", "profile_version"], "left")

#             # Optimized final transformations using selectExpr where possible
#             df_final = combined_df.selectExpr(
#                 "profile_id",
#                 "profile_version",
#                 """
#                 element_at(
#                     array_distinct(
#                         filter(
#                             transform(name, x -> 
#                                 trim(concat_ws(' ', 
#                                     CASE WHEN trim(x.title) != '' THEN x.title END,
#                                     x.first_name,
#                                     CASE WHEN trim(x.middle_name) != '' THEN x.middle_name END,
#                                     x.last_name
#                                 ))
#                             ),
#                             y -> y IS NOT NULL AND trim(y) != ''
#                         )
#                     ), 1
#                 ) as name
#                 """,
#                 "struct(" +
#                     "CASE WHEN emails IS NOT NULL " +
#                     "THEN transform(emails, x -> struct(x.email_address as email_address, x.source_id as source_id, x.source_type as source_type)) " +
#                     "ELSE array() END as emails, " +
#                     "CASE WHEN phones IS NOT NULL " +
#                     "THEN transform(phones, x -> struct(x.phone_number as phone_number, x.source_id as source_id, x.source_type as source_type)) " +
#                     "ELSE array() END as phones, " +
#                     "CASE WHEN addresses IS NOT NULL " +
#                     "THEN transform(addresses, x -> struct(" +
#                         "x.city as city, x.state as state, x.country as country, " +
#                         "x.country_code as country_code, x.state_code as state_code, " +
#                         "x.postal_code as postal_code, x.postal_box as postal_box, " +
#                         "x.source_id as source_id, x.source_type as source_type" +
#                     ")) ELSE array() END as addresses" +
#                 ") as contact_details",
#                 """
#                 CASE WHEN demographics IS NOT NULL
#                 THEN transform(demographics, x -> struct(
#                     x.birth_date as birth_date,
#                     x.birth_country as birth_country,
#                     x.birth_place as birth_place,
#                     x.nationalities as nationality,
#                     x.gender as gender,
#                     x.civil_status as civil_status,
#                     x.source_id as source_id,
#                     x.source_type as source_type
#                 ))
#                 ELSE array() END as demographics
#                 """,
#                 """
#                 CASE WHEN documents IS NOT NULL
#                 THEN transform(documents, x -> struct(
#                     x.document_type as document_type,
#                     x.number as number,
#                     x.gender as gender,
#                     x.issuance_country as issuance_country,
#                     x.issuance_date as issuance_date,
#                     x.issuance_location as issuance_location,
#                     x.expiry_date as expiry_date,
#                     x.birth_date as birth_date,
#                     x.birth_country as birth_country,
#                     x.birth_place as birth_place,
#                     x.civil_status as civil_status,
#                     x.label as label,
#                     x.nationality as nationality,
#                     x.effective_date as effective_date,
#                     x.rank as rank,
#                     x.remarks as remarks,
#                     x.source_id as source_id,
#                     x.source_type as source_type
#                 ))
#                 ELSE array() END as identity_documents
#                 """,
#                 """
#                 CASE WHEN metrics IS NOT NULL
#                 THEN array_distinct(transform(metrics, x -> struct(
#                     x.code as code,
#                     x.name as name,
#                     x.value as value,
#                     x.source_id as source_id,
#                     x.source_type as source_type
#                 )))
#                 ELSE array() END as metrics
#                 """,
#                 """
#                 CASE WHEN memberships IS NOT NULL
#                 THEN transform(memberships, x -> struct(
#                     x.membership_type as membership_type,
#                     x.member_id as member_id,
#                     x.membership_rank as membership_rank,
#                     x.program_owner as program_owner,
#                     x.active_tier_name as active_tier_name,
#                     x.active_tier_code as active_tier_code,
#                     x.active_tier_priority as active_tier_priority,
#                     x.alliance_tier_name as alliance_tier_name,
#                     x.alliance_tier_code as alliance_tier_code,
#                     x.alliance_tier_priority as alliance_tier_priority,
#                     x.source_id as source_id,
#                     x.source_type as source_type
#                 ))
#                 ELSE array() END as memberships
#                 """,
#                 """
#                 CASE WHEN dictionary IS NOT NULL
#                 THEN transform(dictionary, x -> struct(
#                     x.ticketing_last_modified_datetime as ticketing_last_modified_datetime,
#                     x.ticketing_received_datetime as ticketing_received_datetime,
#                     x.ticket_id as ticket_id,
#                     x.ticket_number as ticket_number,
#                     x.pnr_creation_datetime as pnr_creation_datetime,
#                     x.pnr_last_modified_datetime as pnr_last_modified_datetime,
#                     x.pnr_link_id as pnr_link_id,
#                     x.number_in_party as number_in_party,
#                     x.passenger_tattoo as passenger_tattoo,
#                     x.pnr_number as pnr_number,
#                     x.traveler_ids as traveler_ids
#                     --x.dcs_id as dcs_id  
#                 ))
#                 ELSE array() END as dictionary_ids
#                 """
#             ).withColumn("msg_type", F.lit("TDNA")) \
#              .withColumn("tdnakey", col("profile_id")) \
#              .withColumn("combined_dictionary", col("dictionary_ids"))

#             # Final selection with optimized join
#             df_final_with_operation = df_final.join(
#                 tdna_df.select("profile_id", "profile_version", "context_datetime", "context_process", "enqueued_datetime").distinct(),
#                 ["profile_id", "profile_version"],
#                 "left"
#             ).selectExpr(
#                 "profile_id as id",
#                 "tdnakey",
#                 "profile_version",
#                 "context_datetime",
#                 "'TDNA' as msg_type",
#                 "context_process",
#                 "name",
#                 "contact_details",
#                 "demographics",
#                 "identity_documents",
#                 "metrics",
#                 "memberships",
#                 "combined_dictionary as dictionary",
#                 "enqueued_datetime"
#             )

#             if df_final_with_operation is not None:
#                 return df_final_with_operation

#         except Exception as e:
#             error_msg = str(e).lower()
#             if ("reactordispatcher" in error_msg or "closed" in error_msg or
#                 "interrupted" in error_msg or "cancel" in error_msg) and attempt < max_retries - 1:
#                 time.sleep(30 * (attempt + 1))
#                 continue
#             else:
#                 print(f" [{batch_id}] Error in process_tdna_data: {str(e)}")
#                 traceback.print_exc()
#                 return None

# COMMAND ----------

def process_tdna_data(parsed_df, batch_id=None, bridge_table_path=None):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            df_with_context = parsed_df.selectExpr(
                "parsed_json.LiveFeed.customer.profile.id as profile_id",
                "parsed_json.LiveFeed.customer.profile.version as profile_version",
                "parsed_json.LiveFeed.context.timestamp as timestamp",
                "parsed_json.LiveFeed.context.process as context_process",
                f"date_format(to_timestamp(enqueuedTime), 'yyyy-MM-dd\\'T\\'HH:mm:ss\\'Z\\'') as enqueued_datetime",
                "parsed_json"
            )

            w_latest = Window.partitionBy("profile_id").orderBy(expr("cast(profile_version as double) desc"))
            latest_df = df_with_context.withColumn("rn", row_number().over(w_latest)).filter(col("rn") == 1).drop("rn")

            tdna_df = latest_df.selectExpr(
                "profile_id",
                "profile_version",
                "timestamp as context_datetime",
                "context_process",
                "enqueued_datetime",
                "parsed_json.LiveFeed.customer.profile.identity.names as name",
                "parsed_json.LiveFeed.customer.profile.identity.documents as identity_documents",
                "parsed_json.LiveFeed.customer.profile.identity.demographics as demographics",
                "parsed_json.LiveFeed.customer.profile.contact.emails as emails",
                "parsed_json.LiveFeed.customer.profile.contact.phones as phones",
                "parsed_json.LiveFeed.customer.profile.contact.addresses as addresses",
                "parsed_json.LiveFeed.customer.profile.links as links",
                "parsed_json.LiveFeed.customer.profile.metrics as metrics",
                "parsed_json.LiveFeed.customer.profile.memberships as memberships",
                "parsed_json.LiveFeed.dictionary.pnrSources as pnr_sources",
                "parsed_json.LiveFeed.dictionary.ticketingSources as ticketing_sources"
            )

            doc_result = extract_data_with_flattened_sources(
                tdna_df, "identity_documents", "item.sources",
                ["profile_id", "profile_version"],
                [
                    col("item.documentType").alias("document_type"),
                    col("item.number").alias("number"),
                    col("item.gender").alias("gender"),
                    col("item.issuanceCountry").alias("issuance_country"),
                    col("item.issuanceDate").alias("issuance_date"),
                    col("item.issuanceLocation").alias("issuance_location"),
                    col("item.expiryDate").alias("expiry_date"),
                    col("item.birthDate").alias("birth_date"),
                    col("item.birthCountry").alias("birth_country"),
                    col("item.birthPlace").alias("birth_place"),
                    col("item.civilStatus").alias("civil_status"),
                    col("item.label").alias("label"),
                    col("item.nationality").alias("nationality"),
                    col("item.effectiveDate").alias("effective_date"),
                    col("item.rank").alias("rank"),
                    col("item.remarks").alias("remarks")
                ], "documents", None, batch_id, "Documents Processing"
            )

            doc_result = doc_result.withColumn(
                "documents",
                expr("""
                    filter(documents, x -> 
                        lower(x.document_type) in ('passport', 'visa') OR
                        x.document_type in ('PASSPORT', 'VISA')
                    )
                """)
            )

            name_result = extract_data_with_flattened_sources(
                tdna_df, "name", "item.universal.sources",
                ["profile_id", "profile_version"],
                [
                    col("item.universal.firstName").alias("first_name"),
                    col("item.universal.lastName").alias("last_name"),
                    expr("coalesce(trim(element_at(item.universal.titles, 1)), '')").alias("title"),
                    expr("coalesce(trim(element_at(item.universal.middleNames, 1)), '')").alias("middle_name")
                ], "name", None, batch_id, "Names Processing"
            )

            demographics_result = extract_data_with_flattened_sources(
                tdna_df, "demographics", "item.sources",
                ["profile_id", "profile_version"],
                [
                    col("item.birthDate").alias("birth_date"),
                    col("item.birthCountry").alias("birth_country"),
                    col("item.birthPlace").alias("birth_place"),
                    col("item.nationalities").alias("nationalities"),
                    col("item.gender").alias("gender"),
                    col("item.civilStatus").alias("civil_status")
                ], "demographics", None, batch_id, "Demographics Processing"
            )

            email_result = extract_data_with_flattened_sources(
                tdna_df, "emails", "item.sources",
                ["profile_id", "profile_version"],
                [col("item.address").alias("email_address")], "emails", None, batch_id, "Emails Processing"
            )

            phone_result = extract_data_with_flattened_sources(
                tdna_df, "phones", "item.sources",
                ["profile_id", "profile_version"],
                [col("item.number").alias("phone_number")], "phones", None, batch_id, "Phones Processing"
            )
            address_result = extract_data_with_flattened_sources(
                tdna_df, "addresses", "item.sources",
                ["profile_id", "profile_version"],
                [
                    col("item.cityName").alias("city"),
                    col("item.stateName").alias("state"),
                    col("item.countryName").alias("country"),
                    col("item.countryCode").alias("country_code"),
                    col("item.stateCode").alias("state_code"),
                    col("item.postalCode").alias("postal_code"),
                    col("item.postalBox").alias("postal_box")
                ], "addresses", None, batch_id, "Addresses Processing"
            )

            metric_result = tdna_df.select(
                "profile_id",
                "profile_version",
                F.explode_outer("metrics").alias("metric_item")
            ).select(
                "profile_id",
                "profile_version",
                F.explode_outer("metric_item.sources").alias("source"),
                col("metric_item.code").alias("code"),
                col("metric_item.name").alias("name"),
                col("metric_item.value").alias("value")
            ).select(
                "profile_id",
                "profile_version",
                col("source.id").alias("source_id"),
                col("source.sourceType").alias("source_type"),
                "code",
                "name",
                "value"
            ).filter((col("code").isNotNull()) | (col("name").isNotNull()) |(col("value").isNotNull()) | (col("source_id").isNotNull())).distinct().groupBy("profile_id", "profile_version").agg(
                F.collect_list(
                    F.struct(
                        col("code").alias("code"),
                        col("name").alias("name"),
                        col("value").alias("value"),
                        col("source_id").alias("source_id"),
                        col("source_type").alias("source_type")
                    )
                ).alias("metrics")
            )
            all_profiles = tdna_df.select("profile_id", "profile_version").distinct()
            metric_result = all_profiles.join(metric_result,["profile_id", "profile_version"],"left").withColumn("metrics",F.when(F.col("metrics").isNull(), F.array()).otherwise(F.col("metrics")))

            membership_result = extract_data_with_flattened_sources(
                tdna_df, "memberships", "item.sources",
                ["profile_id", "profile_version"],
                [
                    col("item.membershipType").alias("membership_type"),
                    col("item.memberId").alias("member_id"),
                    col("item.rank").alias("membership_rank"),
                    col("item.program.owner").alias("program_owner"),
                    col("item.activeTier.name").alias("active_tier_name"),
                    col("item.activeTier.code").alias("active_tier_code"),
                    col("item.activeTier.priorityCode").alias("active_tier_priority"),
                    col("item.allianceTier.name").alias("alliance_tier_name"),
                    col("item.allianceTier.code").alias("alliance_tier_code"),
                    col("item.allianceTier.priorityCode").alias("alliance_tier_priority")
                ], "memberships", None, batch_id, "Memberships Processing"
            )

            # Process preferences data
            try:
                reference_raw_path = "/Volumes/ops_catalog/cxops/edpppecops_raw/eag/ey/guest_preference_reference_zone/"
                reference_prod_path = "/Volumes/ops_catalog/cxops/edpppecops_product/eag/ey/guest_preference_reference_zone/"
                
                print(f" [{batch_id}] Reading preferences data from {reference_raw_path}")
                reference_df = spark.read.format("json").option("multiline", "true").option("recursiveFileLookup", "true").load(reference_raw_path)
                print(f" [{batch_id}] Preferences columns: {reference_df.columns}")

                # Extract ffpNumber - keep struct for output, create string for Delta storage
                reference_df = reference_df.select(
                    col("preferences.ffpNumber").alias("ffp_number"),
                    col("preferences").alias("preference_json"),
                    F.to_json(col("preferences")).alias("preference_body_string"),
                    F.current_timestamp().alias("ingestion_datetime"),
                    F.current_date().alias("ingestion_date")
                ).filter(col("ffp_number").isNotNull())
                
                pref_count = reference_df.count()
                print(f" [{batch_id}] Valid preferences loaded: {pref_count} records")
                # Explode membership_result to get individual member_ids per profile
                read_from_tdna_df= spark.read.format("delta").load("abfss://prodedp@stedpprod.dfs.corewindows.net//product/eag/ey/TDNA/Trans_TDNAMemberships/")
               
                # Join preferences with membership data on member_id = ffp_number
                joined_preferences = read_from_tdna_df.join(
                    broadcast(reference_df),
                    read_from_tdna_df.member_id == reference_df.ffp_number,
                    "left"
                ).select(
                    "profile_id",
                    "ffp_number",
                    "preference_json",
                    "preference_body_string",
                    "ingestion_datetime",
                    "ingestion_date"
                )
                
                # Write to product zone using MERGE (upsert) operation - use string version
                preference_master_df = joined_preferences.select(
                    "profile_id",
                    "ffp_number",
                    col("preference_body_string").alias("preference_body"),
                    "ingestion_datetime",
                    "ingestion_date"
                )
                
                # # Check if the target table exists
                if DeltaTable.isDeltaTable(spark, reference_prod_path):
                    # Table exists - perform MERGE (upsert)
                    target_table = DeltaTable.forPath(spark, reference_prod_path)
                    
                    target_table.alias("target").merge(
                        preference_master_df.alias("source"),
                        "target.profile_id = source.profile_id AND target.ffp_number = source.ffp_number"
                    ).whenMatchedUpdate(
                        condition="target.preference_body != source.preference_body",
                        set={
                            "preference_body": "source.preference_body",
                            "ingestion_datetime": "source.ingestion_datetime",
                            "ingestion_date": "source.ingestion_date"
                        }
                    ).whenNotMatchedInsertAll().execute()
                    
                    print(f" [{batch_id}] Preferences data merged (upserted) to {reference_prod_path}")
                else:
                    # Table doesn't exist - create it with initial load
                    preference_master_df.write.format("delta").mode("overwrite").save(reference_prod_path)
                    print(f" [{batch_id}] Preferences table created at {reference_prod_path}")
                
                # Aggregate preferences per profile - use struct array for final output
                preferences_result = joined_preferences.groupBy("profile_id").agg(F.collect_list("preference_json").alias("preferences"))
                
                
                # Ensure all profiles have preferences field (empty array if null)
                preferences_result = all_profiles.join(preferences_result, ["profile_id"], "left").withColumn(
                    "preferences",
                    F.when(F.col("preferences").isNull(), F.array()).otherwise(F.col("preferences")))
                
                
            except Exception as pref_err:
                print(f" [{batch_id}] Warning: Could not process preferences data: {str(pref_err)}")
                traceback.print_exc()
                # Create empty preferences result with empty array
                preferences_result = all_profiles.withColumn("preferences", F.array())
            try:
                # Combine explosions where possible
                link_exploded = tdna_df.selectExpr(
                    "profile_id",
                    "profile_version",
                    "explode_outer(links) as link_item"
                ).selectExpr(
                    "profile_id",
                    "profile_version",
                    "explode_outer(link_item.sources) as link_source",
                    "link_item.link.pnr.id as pnr_link_id",
                    "link_item.link.ticketing.ticket.number as ticketing_link_ticket_number",
                    "link_item.link.pnr.booking.locator.internal.value as link_pnr_internal_locator",
                    "link_item.link.pnr.booking.locator.external.value as link_pnr_external_locator"
                ).selectExpr(
                    "profile_id",
                    "profile_version",
                    "pnr_link_id",
                    "ticketing_link_ticket_number",
                    "link_pnr_internal_locator",
                    "link_pnr_external_locator",
                    "link_source.id as link_source_id",
                    "link_source.sourceType as link_source_type"
                )

                # Optimized PNR extraction
                pnr_exploded = tdna_df.selectExpr(
                    "profile_id",
                    "profile_version",
                    "explode_outer(pnr_sources) as pnr_item"
                ).selectExpr(
                    "profile_id",
                    "profile_version",
                    "pnr_item.id as pnr_id",
                    "pnr_item.booking.createdAt as pnr_creation_datetime",
                    "pnr_item.booking.lastModifiedAt as pnr_last_modified_datetime",
                    "pnr_item.booking.numberInParty as pnr_number_in_party",
                    "pnr_item.passengerTattoo as pnr_passenger_tattoo",
                    "pnr_item.booking.locator.internal.value as pnr_internal_locator",
                    "pnr_item.booking.locator.external.value as pnr_external_locator"
                ).distinct()

                # Optimized ticketing extraction
                ticketing_exploded = tdna_df.selectExpr(
                    "profile_id",
                    "profile_version",
                    "explode_outer(ticketing_sources) as ticketing_item"
                ).selectExpr(
                    "profile_id",
                    "profile_version",
                    "ticketing_item.id as ticketing_id",
                    "ticketing_item.lastModifiedAt as ticketing_last_modified_datetime",
                    "ticketing_item.receivedAt as ticketing_received_datetime",
                    "ticketing_item.ticket.number as ticketing_ticket_number"
                ).distinct()

                all_pnrs_with_links = pnr_exploded.alias("p").join(
                    link_exploded.alias("l"),
                    expr("""
                        p.profile_id = l.profile_id AND 
                        p.profile_version = l.profile_version AND 
                        (p.pnr_id = l.link_source_id OR p.pnr_id = l.pnr_link_id)
                    """),
                    "left"
                ).selectExpr(
                    "p.profile_id",
                    "p.profile_version",
                    "p.pnr_id",
                    "p.pnr_creation_datetime",
                    "p.pnr_last_modified_datetime",
                    "p.pnr_number_in_party",
                    "p.pnr_passenger_tattoo",
                    "p.pnr_internal_locator",
                    "p.pnr_external_locator",
                    "l.ticketing_link_ticket_number"
                ).distinct()

                final_combined_matches = all_pnrs_with_links.alias("a").join(
                    ticketing_exploded.alias("t"),
                    expr("""
                        a.profile_id = t.profile_id AND 
                        a.profile_version = t.profile_version AND 
                        a.ticketing_link_ticket_number = t.ticketing_ticket_number
                    """),
                    "left"
                ).selectExpr(
                    "a.profile_id",
                    "a.profile_version",
                    "a.pnr_id",
                    "a.pnr_creation_datetime",
                    "a.pnr_last_modified_datetime",
                    "a.pnr_number_in_party",
                    "a.pnr_passenger_tattoo",
                    "a.pnr_internal_locator",
                    "a.pnr_external_locator",
                    "t.ticketing_id",
                    "t.ticketing_last_modified_datetime",
                    "t.ticketing_received_datetime",
                    "t.ticketing_ticket_number"
                ).distinct()

                # Step 6: Create traveler_id
                combined_with_traveler_id = final_combined_matches.withColumn(
                    "pnr_number", 
                    coalesce(col("pnr_internal_locator"), col("pnr_external_locator"))
                ).withColumn(
                    "traveler_id",
                    when(
                        col("pnr_number").isNotNull() & 
                        col("pnr_creation_datetime").isNotNull() & 
                        col("pnr_passenger_tattoo").isNotNull(),
                        concat_ws(
                            "-",
                            col("pnr_number"),
                            date_format(to_date(col("pnr_creation_datetime")), "yyyy-MM-dd"),
                            concat_ws("-", lit("PT"), col("pnr_passenger_tattoo"))
                        )
                    ).otherwise(lit(None))
                )
                
                # # Step 7: Read bridge table and join to get dcs_id
                # dcs_id_column = lit(None).cast("string").alias("dcs_id")
                
                # if bridge_table_path:
                #     try:
                #         # Read the bridge table
                #         bridge_df = spark.read.format("delta").load("/Volumes/ops_catalog/cxops/edpppecops_product/guest_hub/bridge_table/")
                        
                #         # Join with bridge table on traveler_id
                #         combined_with_dcs = combined_with_traveler_id.join(
                #             bridge_df.select("traveler_id", "dcs_id").distinct(),
                #             on="traveler_id",
                #             how="left"
                #         )
                #     except Exception as bridge_err:
                #         print(f" [{batch_id}] Warning: Could not read bridge table: {str(bridge_err)}")
                #         # If bridge table can't be read, add null dcs_id column
                #         combined_with_dcs = combined_with_traveler_id.withColumn("dcs_id", lit(None))
                # else:
                #     # If no bridge table path provided, add null dcs_id column
                #     combined_with_dcs = combined_with_traveler_id.withColumn("dcs_id", lit(None))

                # Step 8: Create final dictionary structure with dcs_id
                combined_dict_df = combined_with_traveler_id.select(
                    "profile_id", 
                    "profile_version",
                    struct(
                        col("ticketing_last_modified_datetime").alias("ticketing_last_modified_datetime"),
                        col("ticketing_received_datetime").alias("ticketing_received_datetime"),
                        col("ticketing_id").alias("ticket_id"),
                        col("ticketing_ticket_number").alias("ticket_number"),
                        col("pnr_creation_datetime").alias("pnr_creation_datetime"),
                        col("pnr_last_modified_datetime").alias("pnr_last_modified_datetime"),
                        col("pnr_id").alias("pnr_link_id"),
                        col("pnr_number_in_party").alias("number_in_party"),
                        col("pnr_passenger_tattoo").alias("passenger_tattoo"),
                        col("pnr_number").alias("pnr_number"),
                        col("traveler_id").alias("traveler_ids")
                        #col("dcs_id").alias("dcs_id")  
                    ).alias("data")
                ).distinct().groupBy("profile_id", "profile_version").agg(
                    F.collect_set("data").alias("dictionary")
                )
   
            except Exception as e:
                print(f" [{batch_id}] Error processing dictionary data: {str(e)}")
                traceback.print_exc()
                combined_dict_df = tdna_df.select("profile_id", "profile_version").distinct().withColumn(
                    "dictionary", F.array().cast(ArrayType(StructType()))
                )
                print(f" [{batch_id}] Empty dictionary created: {combined_dict_df.count()} records")

            # Optimized joins by starting with all results and reducing joins
            all_results = [name_result, doc_result, demographics_result, email_result,
                          phone_result, address_result, metric_result, membership_result, combined_dict_df]#preferences_result

            # Start with base and sequentially join
            base_profiles = tdna_df.select("profile_id", "profile_version").distinct()
            combined_df = base_profiles
            for result_df in all_results:
                combined_df = combined_df.join(result_df, ["profile_id", "profile_version"], "left")

            # Optimized final transformations using selectExpr where possible
            df_final = combined_df.selectExpr(
                "profile_id",
                "profile_version",
                """
                element_at(
                    array_distinct(
                        filter(
                            transform(name, x -> 
                                trim(concat_ws(' ', 
                                    CASE WHEN trim(x.title) != '' THEN x.title END,
                                    x.first_name,
                                    CASE WHEN trim(x.middle_name) != '' THEN x.middle_name END,
                                    x.last_name
                                ))
                            ),
                            y -> y IS NOT NULL AND trim(y) != ''
                        )
                    ), 1
                ) as name
                """,
                "struct(" +
                    "CASE WHEN emails IS NOT NULL " +
                    "THEN transform(emails, x -> struct(x.email_address as email_address, x.source_id as source_id, x.source_type as source_type)) " +
                    "ELSE array() END as emails, " +
                    "CASE WHEN phones IS NOT NULL " +
                    "THEN transform(phones, x -> struct(x.phone_number as phone_number, x.source_id as source_id, x.source_type as source_type)) " +
                    "ELSE array() END as phones, " +
                    "CASE WHEN addresses IS NOT NULL " +
                    "THEN transform(addresses, x -> struct(" +
                        "x.city as city, x.state as state, x.country as country, " +
                        "x.country_code as country_code, x.state_code as state_code, " +
                        "x.postal_code as postal_code, x.postal_box as postal_box, " +
                        "x.source_id as source_id, x.source_type as source_type" +
                    ")) ELSE array() END as addresses" +
                ") as contact_details",
                """
                CASE WHEN demographics IS NOT NULL
                THEN transform(demographics, x -> struct(
                    x.birth_date as birth_date,
                    x.birth_country as birth_country,
                    x.birth_place as birth_place,
                    x.nationalities as nationality,
                    x.gender as gender,
                    x.civil_status as civil_status,
                    x.source_id as source_id,
                    x.source_type as source_type
                ))
                ELSE array() END as demographics
                """,
                """
                CASE WHEN documents IS NOT NULL
                THEN transform(documents, x -> struct(
                    x.document_type as document_type,
                    x.number as number,
                    x.gender as gender,
                    x.issuance_country as issuance_country,
                    x.issuance_date as issuance_date,
                    x.issuance_location as issuance_location,
                    x.expiry_date as expiry_date,
                    x.birth_date as birth_date,
                    x.birth_country as birth_country,
                    x.birth_place as birth_place,
                    x.civil_status as civil_status,
                    x.label as label,
                    x.nationality as nationality,
                    x.effective_date as effective_date,
                    x.rank as rank,
                    x.remarks as remarks,
                    x.source_id as source_id,
                    x.source_type as source_type
                ))
                ELSE array() END as identity_documents
                """,
                """
                CASE WHEN metrics IS NOT NULL
                THEN array_distinct(transform(metrics, x -> struct(
                    x.code as code,
                    x.name as name,
                    x.value as value,
                    x.source_id as source_id,
                    x.source_type as source_type
                )))
                ELSE array() END as metrics
                """,
                """
                CASE WHEN memberships IS NOT NULL
                THEN transform(memberships, x -> struct(
                    x.membership_type as membership_type,
                    x.member_id as member_id,
                    x.membership_rank as membership_rank,
                    x.program_owner as program_owner,
                    x.active_tier_name as active_tier_name,
                    x.active_tier_code as active_tier_code,
                    x.active_tier_priority as active_tier_priority,
                    x.alliance_tier_name as alliance_tier_name,
                    x.alliance_tier_code as alliance_tier_code,
                    x.alliance_tier_priority as alliance_tier_priority,
                    x.source_id as source_id,
                    x.source_type as source_type
                ))
                ELSE array() END as memberships
                """,
                """
                CASE WHEN dictionary IS NOT NULL
                THEN transform(dictionary, x -> struct(
                    x.ticketing_last_modified_datetime as ticketing_last_modified_datetime,
                    x.ticketing_received_datetime as ticketing_received_datetime,
                    x.ticket_id as ticket_id,
                    x.ticket_number as ticket_number,
                    x.pnr_creation_datetime as pnr_creation_datetime,
                    x.pnr_last_modified_datetime as pnr_last_modified_datetime,
                    x.pnr_link_id as pnr_link_id,
                    x.number_in_party as number_in_party,
                    x.passenger_tattoo as passenger_tattoo,
                    x.pnr_number as pnr_number,
                    x.traveler_ids as traveler_ids
                    --x.dcs_id as dcs_id  
                ))
                ELSE array() END as dictionary_ids
                """,
                # "preferences"
            ).withColumn("msg_type", F.lit("TDNA")) \
             .withColumn("tdnakey", col("profile_id")) \
             .withColumn("combined_dictionary", col("dictionary_ids"))

            # Final selection with optimized join
            df_final_with_operation = df_final.join(
                tdna_df.select("profile_id", "profile_version", "context_datetime", "context_process", "enqueued_datetime").distinct(),
                ["profile_id", "profile_version"],
                "left"
            ).selectExpr(
                "profile_id as id",
                "tdnakey",
                "profile_version",
                "context_datetime",
                "'TDNA' as msg_type",
                "context_process",
                "name",
                "contact_details",
                "demographics",
                "identity_documents",
                "metrics",
                "memberships",
                "combined_dictionary as dictionary",
                #"preferences",
                "enqueued_datetime"
            )

            if df_final_with_operation is not None:
                return df_final_with_operation

        except Exception as e:
            error_msg = str(e).lower()
            if ("reactordispatcher" in error_msg or "closed" in error_msg or
                "interrupted" in error_msg or "cancel" in error_msg) and attempt < max_retries - 1:
                time.sleep(30 * (attempt + 1))
                continue
            else:
                print(f" [{batch_id}] Error in process_tdna_data: {str(e)}")
                traceback.print_exc()
                return None

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, current_timestamp,
    explode_outer, from_json, coalesce,
    date_format, to_date, concat_ws,
    row_number, when
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def extract_json_body_fields(
    streaming_df,
    batch_id,
    raw_dir=None,
    product_dir=None,
    bridge_table_path=None,
    logger=None
):
    try:
        parsed_df = streaming_df.withColumn(
            "parsed_json",
            from_json(col("json_body"), json_schema)
        )

        extracted_df = parsed_df.select(
            "*",
            col("parsed_json.LiveFeed.customer.profile.id").alias("profile_id"),
            col("parsed_json.LiveFeed.customer.profile.version").alias("profile_version"),
            col("parsed_json.LiveFeed.context.timestamp").alias("context_timestamp"),
            element_at(col("parsed_json.LiveFeed.context.dynamicProcesses"), 1)["processName"]
                .alias("dynamic_process_name"),
            element_at(col("parsed_json.LiveFeed.context.dynamicProcesses"), 1)["fromCustomer"]
                .alias("from_customer_ids"),
            element_at(col("parsed_json.LiveFeed.context.dynamicProcesses"), 1)["toCustomer"]
                .alias("to_customer_ids"),
            col("parsed_json.LiveFeed.dictionary.pnrSources").alias("pnrSources"),
            col("parsed_json.LiveFeed.customer.profile.links").alias("links"),
            lit(batch_id).alias("batch_id")
        )
        #product layer
        if product_dir:
            product_df = (
                extracted_df
                .select(
                    col("profile_id").alias("tdnakey"),
                    "profile_version",
                    col("context_timestamp").alias("context_datetime"),
                    "enqueuedTime",
                    current_timestamp().alias("ingestion_datetime"),
                    "dynamic_process_name",
                    concat_ws(",", col("from_customer_ids")).alias("from_customer_ids"),
                    concat_ws(",", col("to_customer_ids")).alias("to_customer_ids"),
                    current_date().alias("ingest_date")
                )
                
            )

            (
                product_df.write
                .format("delta")
                .option("mergeSchema", "true")
                .mode("append")
                .partitionBy("ingest_date")
                .save(product_dir)
            )
        #bridge table
        if bridge_table_path:
            try:
                base_bridge_df = extracted_df.select(
                    col("profile_id").alias("tdna_profile_id"),
                    "pnrSources",
                    "links",
                    col("context_timestamp").alias("last_modification_datetime")
                )

                # ---- explode pnrSources
                pnr_df = (
                    base_bridge_df
                    .withColumn("pnr_item", explode_outer("pnrSources"))
                    .select(
                        "tdna_profile_id",
                        col("pnr_item").alias("pnr_json"),
                        "last_modification_datetime"
                    )
                )

                pnr_df = pnr_df.select(
                    "tdna_profile_id",
                    col("pnr_json.booking.createdAt").alias("pnr_creation_datetime"),
                    col("pnr_json.passengerTattoo").alias("pnr_passengerTattoo"),
                    col("pnr_json.booking.locator.internal.value").alias("pnr_internal_locator"),
                    col("pnr_json.booking.locator.external.value").alias("pnr_external_locator"),
                    "last_modification_datetime"
                )

                # ---- explode links
                links_df = (
                    base_bridge_df
                    .withColumn("link_item", explode_outer("links"))
                    .select(
                        "tdna_profile_id",
                        col("link_item.link.pnr.id").alias("pnr_link_id")
                    )
                )

                # ---- join
                bridge_df = pnr_df.join(
                    links_df,
                    "tdna_profile_id",
                    "left"
                )

                # ---- derive business columns
                bridge_final_df = (
                    bridge_df
                    .withColumn(
                        "pnr_number",
                        coalesce(col("pnr_internal_locator"), col("pnr_external_locator"))
                    )
                    .withColumn(
                        "traveler_id",
                        when(
                            col("pnr_number").isNotNull()
                            & col("pnr_creation_datetime").isNotNull()
                            & col("pnr_passengerTattoo").isNotNull(),
                            concat_ws(
                                "-",
                                col("pnr_number"),
                                date_format(to_date(col("pnr_creation_datetime")), "yyyy-MM-dd"),
                                concat_ws("-", lit("PT"), col("pnr_passengerTattoo"))
                            )
                        )
                    )
                    .withColumn("last_modification_date",to_date(col("last_modification_datetime")))
                    .select(
                        "tdna_profile_id",
                        "pnr_number",
                        "traveler_id",
                        "last_modification_datetime",  
                        "last_modification_date"     
                    )
                )

                # ---- deduplicate (latest record)
                win = (
                    Window
                    .partitionBy("pnr_number", "traveler_id")
                    .orderBy(col("last_modification_datetime").desc())
                )

                bridge_dedup_df = (
                    bridge_final_df
                    .withColumn("rn", row_number().over(win))
                    .filter(col("rn") == 1)
                    .drop("rn")
                )
                if DeltaTable.isDeltaTable(spark, bridge_table_path):
                    (
                        bridge_dedup_df.write
                        .format("delta")
                        .partitionBy("last_modification_date")
                        .option("mergeSchema", "true")
                        .mode("append")
                        .save(bridge_table_path)
                    )
                else:
                    (
                        bridge_dedup_df.write
                        .format("delta")
                        .partitionBy("last_modification_date")
                        .option("mergeSchema", "true")
                        .mode("overwrite")
                        .save(bridge_table_path)
                    )

            except Exception as e:
                if logger:
                    logger.warning(
                        f"[{batch_id}] Bridge processing failed: {str(e)}"
                    )
        return extracted_df

    except Exception as e:
        if logger:
            logger.error(
                f"[{batch_id}] JSON extraction failed: {str(e)}"
            )
        return streaming_df.select("json_body", "enqueuedTime")

# COMMAND ----------

def write_to_cosmos_by_operation(final_result, working_format, cfg, batch_id, logger):
    """
    FINAL VERSION – Returns only total count
    """
    try:
        data_exists = final_result.rdd.isEmpty() == False
        if not data_exists:
            logger.warning(f"[{batch_id}] No data to process for Cosmos DB")
            return 0  

        required_columns = {"tdnakey", "id", "context_process"}
        missing = required_columns - set(final_result.columns)
        if missing:
            logger.error(f"[{batch_id}] Missing required columns: {missing}")
            return 0

        df = final_result.withColumn(
            "process_lower", F.lower(F.col("context_process"))
        ).withColumn(
            "operation",
            F.when(F.col("process_lower").contains("customercreate"), "CustomerCreate")
             .when(F.col("process_lower").contains("customerdelete"), "CustomerDelete")
             .otherwise("CustomerUpdate")
        )


        current_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        write_options = {
            **cfg,
            "spark.cosmos.write.bulk.enabled": "true",
            "spark.cosmos.write.bulk.maxPendingOperations": "5000",
            "spark.cosmos.write.bulk.initialBatchSize": "5000",
            "spark.cosmos.write.maxRetryCount": "5",
            "spark.cosmos.write.strategy": "ItemOverwrite",
        }

        def write_df(df, is_delete=False):
            all_columns = df.columns
            columns_to_select = [col for col in all_columns if col not in ["process_lower", "operation"]]
            out_df = df.select(*columns_to_select).withColumn(
                "cosmos_processed_datetime", F.lit(current_utc)
            )
            if is_delete:
                out_df = (
                    out_df
                    .withColumn("is_deleted", F.lit(True))
                    .withColumn("deleted_datetime", F.lit(current_utc))
                )
            out_df = out_df.repartition(16)
            (out_df.write
                .format(working_format)
                .options(**write_options)
                .mode("append")
                .save())


        total_count = 0
        
        ops = ["CustomerCreate", "CustomerUpdate", "CustomerDelete"]
        for op in ops:
            subset = df.filter(F.col("operation") == op)
            if not subset.rdd.isEmpty():
                write_df(subset, is_delete=(op == "CustomerDelete"))
                total_count += subset.count()

        return total_count  

    except Exception as e:
        logger.error(f"[{batch_id}] Cosmos write failed: {str(e)}", exc_info=True)
        return 0 


def safe_log_batch_info(delta_path, module_name, batch_id, input_count, start_time, end_time,
                        processed_count, skipped_count, dict_update_time, duration):
    try:
        row = Row(
            module_name=str(module_name),
            batch_id=str(batch_id),
            input_count=str(input_count),
            start_time=str(start_time),
            end_time=str(end_time),
            processed_count=str(processed_count),
            skipped_count=str(skipped_count),
            dict_update_time=str(dict_update_time),
            duration=str(duration)
        )
        df = spark.createDataFrame([row])
        (df.write
           .format("delta")
           .option("mergeSchema", "true")
           .mode("append")
           .save(delta_path))
    except Exception as e:
        print(f"FALLBACK LOG ERROR - Batch {batch_id}: Failed to log batch info: {str(e)}")

def safe_log_exception_info(delta_path, module_name, batch_id, start_time, end_time, message):
    try:
        row = Row(
            module_name=str(module_name),
            batch_id=str(batch_id),
            start_time=str(start_time),
            end_time=str(end_time),
            message=str(message)[:1000]
        )
        df = spark.createDataFrame([row])
        (df.write
           .format("delta")
           .option("mergeSchema", "true")
           .mode("append")
           .save(delta_path))
    except Exception as e:
        print(f"FALLBACK EXC LOG - Batch {batch_id}: {message}")

# COMMAND ----------

def process_batch_with_dependencies(
    batch_df, batch_id,
    raw_dir, product_dir, bridge_table_path,
    json_schema, cosmos_format, cosmos_config,
    log_directory_batch, log_directory_exception,
    logger
):
    from datetime import datetime
    from builtins import sum as py_sum

    batch_start_time = datetime.now()

    try:
        extracted_df = extract_json_body_fields(
            batch_df, batch_id,
            raw_dir, product_dir, bridge_table_path, logger
        )
        # extracted_df = batch_df.withColumn(
        #     "parsed_json",
        #     from_json(col("json_body"), json_schema)
        # )

        final_result = process_tdna_data(
            extracted_df, batch_id, bridge_table_path
        )

        event_hub_count = batch_df.count()
        print(f"[{batch_id}] Event Hub input count: {event_hub_count}")

        if event_hub_count == 0 or final_result is None:
            safe_log_batch_info(
                log_directory_batch, "TDNA_Processor",
                batch_id, 0, batch_start_time, datetime.now(),
                0, 0, datetime.now(), 0
            )
            print(f"[{batch_id}] No data processed for Cosmos.")
            return {"batch_id": batch_id}

        cosmos_count = write_to_cosmos_by_operation(
            final_result, cosmos_format, cosmos_config, batch_id, logger
        )
        print(f"[{batch_id}] Cosmos output count: {cosmos_count}")

        input_count = event_hub_count

        batch_end_time = datetime.now()
        duration = (batch_end_time - batch_start_time).total_seconds()

        safe_log_batch_info(
            log_directory_batch, "TDNA_Processor",
            batch_id, input_count, batch_start_time, batch_end_time,
            cosmos_count, input_count - cosmos_count,
            batch_end_time, duration
        )
        return {"batch_id": batch_id, "cosmos_count": cosmos_count}

    except Exception as e:
        err_msg = f"[{batch_id}] Error: {str(e)}"
        logger.error(err_msg, exc_info=True)

        safe_log_exception_info(
            log_directory_exception, "TDNA_Processor",
            batch_id, batch_start_time, datetime.now(), err_msg
        )

        return {"batch_id": batch_id, "error": str(e)}	