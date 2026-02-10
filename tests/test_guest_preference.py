import pytest
from unittest.mock import patch, MagicMock, call
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import functions as F


@pytest.fixture
def spark():
    return SparkSession.builder.appName("unit-tests").getOrCreate()


@pytest.fixture
def sample_reference_df(spark):
    """Sample reference data based on your input sample"""
    schema = StructType([
        StructField("preferences", StructType([
            StructField("ffpNumber", StringType()),
            StructField("personalInformation", StructType([
                StructField("title", StringType()),
                StructField("fullName", StringType()),
                StructField("preferredFormOfAddress", StringType()),
                StructField("dob", StringType()),
                StructField("preferredLanguage", StringType()),
                StructField("detailsToKnowRegularly", StringType()),
            ])),
            StructField("diningPreferences", StructType([
                StructField("allergies", StringType()),
                StructField("waterPreference", StringType()),
                StructField("welcomeBeverage", StringType()),
                StructField("otherBeverages", StringType()),
                StructField("hotBeverage", StringType()),
                StructField("meatPreference", StringType()),
            ])),
            StructField("productAndServiceDelivery", StructType([
                StructField("standardItems", StringType()),
                StructField("amenities", StringType()),
                StructField("loungewearSize", StringType()),
                StructField("slippersSize", StringType()),
            ])),
            StructField("expectations", ArrayType(StringType())),
        ])),
    ])
    data = [
        {
            "preferences": {
                "ffpNumber": "520000307470",
                "personalInformation": {
                    "title": "Mr.",
                    "fullName": "JOHN TIM",
                    "preferredFormOfAddress": "Mr. Test1",
                    "dob": "09-02",
                    "preferredLanguage": "English",
                    "detailsToKnowRegularly": "Flight time and delays (if applicable)"
                },
                "diningPreferences": {
                    "allergies": "None",
                    "waterPreference": "Sparkling",
                    "welcomeBeverage": "None",
                    "otherBeverages": "Non - alcoholic",
                    "hotBeverage": "None",
                    "meatPreference": "Medium"
                },
                "productAndServiceDelivery": {
                    "standardItems": "to be used for bedding",
                    "amenities": "to be offered by hand",
                    "loungewearSize": "M/L",
                    "slippersSize": "37 - 44"
                },
                "expectations": [
                    "No Meet and Greet at the airport",
                    "Guest prefers not to be surrounded by people in the airport"
                ]
            }
        }
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_tdna_membership_df(spark):
    """Sample TDNA membership data"""
    schema = StructType([
        StructField("profile_id", StringType()),
        StructField("memberId", StringType()),
    ])
    data = [
        {"profile_id": "P123", "memberId": "520000307470"}
    ]
    return spark.createDataFrame(data, schema)


def test_business_logic_transformation(spark, sample_reference_df, sample_tdna_membership_df):
    """Test the core transformation logic used in business logic"""
    # Test the main transformation logic from your business logic
    reference_df = (
        sample_reference_df
        .select(
            F.col("preferences.ffpNumber").alias("ffp_number"),
            F.array(
                F.struct(
                    F.col("preferences.ffpNumber").alias("ffpNumber"),
                    F.col("preferences.personalInformation").alias("personalInformation"),
                    F.col("preferences.diningPreferences").alias("diningPreferences"),
                    F.col("preferences.productAndServiceDelivery").alias("productAndServiceDelivery"),
                    F.col("preferences.expectations").alias("expectations")
                )
            ).alias("preference_body"),
            F.current_timestamp().alias("ingestion_datetime"),
            F.current_date().alias("ingestion_date")
        )
        .filter(F.col("ffp_number").isNotNull())
    )
    
    read_from_tdna_df = (
        sample_tdna_membership_df
        .select(
            F.col("profile_id"),
            F.col("memberId").alias("ffp_number")
        )
    )
    
    preferences_with_profile = (
        F.broadcast(reference_df)
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
    
    # Verify the results
    result = preferences_with_profile.collect()
    assert len(result) == 1
    assert result[0]["profile_id"] == "P123"
    assert result[0]["ffp_number"] == "520000307470"
    assert result[0]["preference_body"][0]["ffpNumber"] == "520000307470"
    assert result[0]["preference_body"][0]["personalInformation"]["fullName"] == "JOHN TIM"
    assert len(result[0]["preference_body"][0]["expectations"]) == 2


def test_null_ffp_number_filtered(spark):
    """Test that records with null ffpNumber are filtered out"""
    schema = StructType([
        StructField("preferences", StructType([
            StructField("ffpNumber", StringType())
        ]))
    ])
    data = [{"preferences": {"ffpNumber": None}}]
    df = spark.createDataFrame(data, schema)
    
    reference_df = (
        df
        .select(
            F.col("preferences.ffpNumber").alias("ffp_number"),
            F.array(
                F.struct(
                    F.col("preferences.ffpNumber").alias("ffpNumber")
                )
            ).alias("preference_body"),
            F.current_timestamp().alias("ingestion_datetime"),
            F.current_date().alias("ingestion_date")
        )
        .filter(F.col("ffp_number").isNotNull())
    )
    
    assert reference_df.count() == 0


def test_multiple_records_processing(spark, sample_tdna_membership_df):
    """Test processing multiple preference records"""
    schema = StructType([
        StructField("preferences", StructType([
            StructField("ffpNumber", StringType()),
            StructField("personalInformation", StructType([
                StructField("fullName", StringType())
            ])),
            StructField("diningPreferences", StructType([])),
            StructField("productAndServiceDelivery", StructType([])),
            StructField("expectations", ArrayType(StringType()))
        ]))
    ])
    data = [
        {
            "preferences": {
                "ffpNumber": "520000307470",
                "personalInformation": {"fullName": "JOHN TIM"},
                "diningPreferences": {},
                "productAndServiceDelivery": {},
                "expectations": []
            }
        },
        {
            "preferences": {
                "ffpNumber": "999999999999",
                "personalInformation": {"fullName": "JANE DOE"},
                "diningPreferences": {},
                "productAndServiceDelivery": {},
                "expectations": []
            }
        }
    ]
    df = spark.createDataFrame(data, schema)
    
    reference_df = (
        df
        .select(
            F.col("preferences.ffpNumber").alias("ffp_number"),
            F.array(
                F.struct(
                    F.col("preferences.ffpNumber").alias("ffpNumber"),
                    F.col("preferences.personalInformation").alias("personalInformation"),
                    F.col("preferences.diningPreferences").alias("diningPreferences"),
                    F.col("preferences.productAndServiceDelivery").alias("productAndServiceDelivery"),
                    F.col("preferences.expectations").alias("expectations")
                )
            ).alias("preference_body"),
            F.current_timestamp().alias("ingestion_datetime"),
            F.current_date().alias("ingestion_date")
        )
        .filter(F.col("ffp_number").isNotNull())
    )
    
    read_from_tdna_df = (
        sample_tdna_membership_df
        .select(
            F.col("profile_id"),
            F.col("memberId").alias("ffp_number")
        )
    )
    
    preferences_with_profile = (
        F.broadcast(reference_df)
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
    
    result = preferences_with_profile.collect()
    assert len(result) == 2
    # Only one should have profile_id as it matches TDNA data
    profile_ids = [r["profile_id"] for r in result]
    assert "P123" in profile_ids
    assert None in profile_ids


def test_duplicate_records_distinct(spark, sample_tdna_membership_df):
    """Test that duplicate records are handled by distinct()"""
    schema = StructType([
        StructField("preferences", StructType([
            StructField("ffpNumber", StringType()),
            StructField("personalInformation", StructType([
                StructField("fullName", StringType())
            ])),
            StructField("diningPreferences", StructType([])),
            StructField("productAndServiceDelivery", StructType([])),
            StructField("expectations", ArrayType(StringType()))
        ]))
    ])
    # Same record twice
    data = [
        {
            "preferences": {
                "ffpNumber": "520000307470",
                "personalInformation": {"fullName": "JOHN TIM"},
                "diningPreferences": {},
                "productAndServiceDelivery": {},
                "expectations": []
            }
        },
        {
            "preferences": {
                "ffpNumber": "520000307470",
                "personalInformation": {"fullName": "JOHN TIM"},
                "diningPreferences": {},
                "productAndServiceDelivery": {},
                "expectations": []
            }
        }
    ]
    df = spark.createDataFrame(data, schema)
    
    reference_df = (
        df
        .select(
            F.col("preferences.ffpNumber").alias("ffp_number"),
            F.array(
                F.struct(
                    F.col("preferences.ffpNumber").alias("ffpNumber"),
                    F.col("preferences.personalInformation").alias("personalInformation"),
                    F.col("preferences.diningPreferences").alias("diningPreferences"),
                    F.col("preferences.productAndServiceDelivery").alias("productAndServiceDelivery"),
                    F.col("preferences.expectations").alias("expectations")
                )
            ).alias("preference_body"),
            F.current_timestamp().alias("ingestion_datetime"),
            F.current_date().alias("ingestion_date")
        )
        .filter(F.col("ffp_number").isNotNull())
    )
    
    read_from_tdna_df = (
        sample_tdna_membership_df
        .select(
            F.col("profile_id"),
            F.col("memberId").alias("ffp_number")
        )
    )
    
    preferences_with_profile = (
        F.broadcast(reference_df)
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
    
    result = preferences_with_profile.collect()
    assert len(result) == 1  # Should be deduplicated


def test_empty_inputs(spark):
    """Test behavior with empty input DataFrames"""
    schema = StructType([
        StructField("preferences", StructType([
            StructField("ffpNumber", StringType())
        ]))
    ])
    ref_df = spark.createDataFrame([], schema)
    
    tdna_schema = StructType([
        StructField("profile_id", StringType()),
        StructField("memberId", StringType()),
    ])
    tdna_df = spark.createDataFrame([], tdna_schema)
    
    reference_df = (
        ref_df
        .select(
            F.col("preferences.ffpNumber").alias("ffp_number"),
            F.array(F.struct(F.col("preferences.ffpNumber").alias("ffpNumber"))).alias("preference_body"),
            F.current_timestamp().alias("ingestion_datetime"),
            F.current_date().alias("ingestion_date")
        )
        .filter(F.col("ffp_number").isNotNull())
    )
    
    preferences_with_profile = (
        F.broadcast(reference_df)
        .join(
            tdna_df.withColumnRenamed("memberId", "ffp_number"),
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
    
    assert preferences_with_profile.count() == 0


def test_ingestion_fields_types(spark, sample_reference_df, sample_tdna_membership_df):
    """Test that ingestion fields are of correct types"""
    reference_df = (
        sample_reference_df
        .select(
            F.col("preferences.ffpNumber").alias("ffp_number"),
            F.array(
                F.struct(
                    F.col("preferences.ffpNumber").alias("ffpNumber"),
                    F.col("preferences.personalInformation").alias("personalInformation"),
                    F.col("preferences.diningPreferences").alias("diningPreferences"),
                    F.col("preferences.productAndServiceDelivery").alias("productAndServiceDelivery"),
                    F.col("preferences.expectations").alias("expectations")
                )
            ).alias("preference_body"),
            F.current_timestamp().alias("ingestion_datetime"),
            F.current_date().alias("ingestion_date")
        )
        .filter(F.col("ffp_number").isNotNull())
    )
    
    read_from_tdna_df = (
        sample_tdna_membership_df
        .select(
            F.col("profile_id"),
            F.col("memberId").alias("ffp_number")
        )
    )
    
    preferences_with_profile = (
        F.broadcast(reference_df)
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
    
    row = preferences_with_profile.first()
    # Check timestamp and date types
    from datetime import datetime, date
    assert isinstance(row["ingestion_datetime"], datetime)
    assert isinstance(row["ingestion_date"], date)


def test_empty_expectations_array(spark, sample_tdna_membership_df):
    """Test handling of empty expectations array"""
    schema = StructType([
        StructField("preferences", StructType([
            StructField("ffpNumber", StringType()),
            StructField("personalInformation", StructType([
                StructField("fullName", StringType())
            ])),
            StructField("diningPreferences", StructType([])),
            StructField("productAndServiceDelivery", StructType([])),
            StructField("expectations", ArrayType(StringType()))
        ]))
    ])
    data = [{
        "preferences": {
            "ffpNumber": "520000307470",
            "personalInformation": {"fullName": "JOHN TIM"},
            "diningPreferences": {},
            "productAndServiceDelivery": {},
            "expectations": []
        }
    }]
    df = spark.createDataFrame(data, schema)
    
    reference_df = (
        df
        .select(
            F.col("preferences.ffpNumber").alias("ffp_number"),
            F.array(
                F.struct(
                    F.col("preferences.ffpNumber").alias("ffpNumber"),
                    F.col("preferences.personalInformation").alias("personalInformation"),
                    F.col("preferences.diningPreferences").alias("diningPreferences"),
                    F.col("preferences.productAndServiceDelivery").alias("productAndServiceDelivery"),
                    F.col("preferences.expectations").alias("expectations")
                )
            ).alias("preference_body"),
            F.current_timestamp().alias("ingestion_datetime"),
            F.current_date().alias("ingestion_date")
        )
        .filter(F.col("ffp_number").isNotNull())
    )
    
    read_from_tdna_df = (
        sample_tdna_membership_df
        .select(
            F.col("profile_id"),
            F.col("memberId").alias("ffp_number")
        )
    )
    
    preferences_with_profile = (
        F.broadcast(reference_df)
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
    
    row = preferences_with_profile.first()
    assert row["preference_body"][0]["expectations"] == []


# def test_missing_nested_fields(spark, sample_tdna_membership_df):
#     """Test handling when some nested fields are missing"""
#     schema = StructType([
#         StructField("preferences", StructType([
#             StructField("ffpNumber", StringType())
#         ]))
#     ])
#     data = [{
#         "preferences": {
#             "ffpNumber": "520000307470"
#             # personalInformation, diningPreferences, productAndServiceDelivery, expectations missing
#         }
#     }]
#     df = spark.createDataFrame(data, schema)
    
#     reference_df = (
#         df
#         .select(
#             F.col("preferences.ffpNumber").alias("ffp_number"),
#             F.array(
#                 F.struct(
#                     F.col("preferences.ffpNumber").alias("ffpNumber")
#                 )
#             ).alias("preference_body"),
#             F.current_timestamp().alias("ingestion_datetime"),
#             F.current_date().alias("ingestion_date")
#         )
#         .filter(F.col("ffp_number").isNotNull())
#     )
    
#     read_from_tdna_df = (
#         sample_tdna_membership_df
#         .select(
#             F.col("profile_id"),
#             F.col("memberId").alias("ffp_number")
#         )
#     )
    
#     preferences_with_profile = (
#         F.broadcast(reference_df)
#         .join(
#             read_from_tdna_df,
#             on="ffp_number",
#             how="left"
#         )
#         .select(
#             "profile_id",
#             "ffp_number",
#             "preference_body",
#             "ingestion_datetime",
#             "ingestion_date"
#         )
#         .distinct()
#     )
    
#     row = preferences_with_profile.first()
#     assert row["preference_body"][0]["ffpNumber"] == "520000307470"

# def test_business_logic_overwrite_scenario():
#     """Test business logic when DeltaTable doesn't exist (overwrite scenario)"""
#     # Mock DataFrame behavior
#     mock_df = MagicMock()
#     mock_df.select.return_value = mock_df
#     mock_df.filter.return_value = mock_df
#     mock_df.alias.return_value = mock_df
#     mock_df.join.return_value = mock_df
#     mock_df.distinct.return_value = mock_df
#     mock_df.count.return_value = 42
    
#     # Mock write operations
#     mock_write = MagicMock()
#     mock_df.write = mock_write
#     mock_write.format.return_value = mock_write
#     mock_write.mode.return_value = mock_write
#     mock_write.save.return_value = None
    
#     # Mock spark session
#     mock_spark = MagicMock()
#     mock_read = MagicMock()
#     mock_spark.read = mock_read
#     mock_read.format.return_value = mock_read
#     mock_read.option.return_value = mock_read
#     mock_read.load.return_value = mock_df
    
#     # Mock DeltaTable operations for overwrite scenario
#     mock_delta_table_class = MagicMock()
#     mock_delta_table_class.isDeltaTable.return_value = False  # Table doesn't exist
    
#     # Patch both the spark variable and DeltaTable class
#     with patch("guesthub.guest_preference.business_logic_guest_preference.spark", mock_spark), \
#          patch("guesthub.guest_preference.business_logic_guest_preference.DeltaTable", mock_delta_table_class), \
#          patch("builtins.print") as mock_print:
        
#         # Import the business logic module
#         import importlib
#         import sys
        
#         # Clear the module from cache if it exists
#         module_name = "guesthub.guest_preference.business_logic_guest_preference"
#         if module_name in sys.modules:
#             del sys.modules[module_name]
        
#         # Import the module (this will execute the top-level code)
#         import guesthub.guest_preference.business_logic_guest_preference
    
#     # Verify DeltaTable.isDeltaTable was called
#     mock_delta_table_class.isDeltaTable.assert_called()
    
#     # Verify that forPath was NOT called (since table doesn't exist)
#     mock_delta_table_class.forPath.assert_not_called()
    
#     # Verify print statements
#     print_calls = [str(call) for call in mock_print.call_args_list]
#     assert any("Master preferences table created at" in call for call in print_calls)


# def test_full_business_logic_execution():
#     """Test full business logic execution with mocks to cover all I/O and DeltaTable operations"""
#     # Mock DataFrame behavior
#     mock_df = MagicMock()
#     mock_df.select.return_value = mock_df
#     mock_df.filter.return_value = mock_df
#     mock_df.alias.return_value = mock_df
#     mock_df.join.return_value = mock_df
#     mock_df.distinct.return_value = mock_df
#     mock_df.count.return_value = 42
    
#     # Mock write operations
#     mock_write = MagicMock()
#     mock_df.write = mock_write
#     mock_write.format.return_value = mock_write
#     mock_write.mode.return_value = mock_write
#     mock_write.save.return_value = None
    
#     # Mock spark session
#     mock_spark = MagicMock()
#     mock_read = MagicMock()
#     mock_spark.read = mock_read
#     mock_read.format.return_value = mock_read
#     mock_read.option.return_value = mock_read
#     mock_read.load.return_value = mock_df
    
#     # Mock DeltaTable
#     mock_delta_table_class = MagicMock()
#     mock_delta_table_class.isDeltaTable.return_value = True
#     mock_target_table = MagicMock()
#     mock_delta_table_class.forPath.return_value = mock_target_table
    
#     # Mock merge chain
#     mock_merge = MagicMock()
#     mock_target_table.alias.return_value = mock_target_table
#     mock_target_table.merge.return_value = mock_merge
#     mock_merge.whenMatchedUpdate.return_value = mock_merge
#     mock_merge.whenNotMatchedInsertAll.return_value = mock_merge
#     mock_merge.execute.return_value = None
    
#     # Patch both the spark variable and DeltaTable class
#     with patch("guesthub.guest_preference.business_logic_guest_preference.spark", mock_spark), \
#          patch("guesthub.guest_preference.business_logic_guest_preference.DeltaTable", mock_delta_table_class), \
#          patch("builtins.print") as mock_print:
        
#         # Import the business logic module, which will execute the top-level code
#         import importlib
#         import sys
        
#         # Clear the module from cache if it exists
#         module_name = "guesthub.guest_preference.business_logic_guest_preference"
#         if module_name in sys.modules:
#             del sys.modules[module_name]
        
#         # Import the module (this will execute the top-level code)
#         import guesthub.guest_preference.business_logic_guest_preference
    
#     # Verify that all the expected operations were called
#     # Verify spark.read was called for both data sources
#     assert mock_read.format.call_count >= 2  # Called for reference_df and tdna_df
#     assert mock_read.load.call_count >= 2
    
#     # Verify DataFrame operations
#     assert mock_df.select.call_count >= 3  # Called multiple times in the pipeline
#     assert mock_df.filter.called
#     assert mock_df.join.called
#     assert mock_df.distinct.called
#     assert mock_df.count.called
    
#     # Verify write operations
#     mock_write.format.assert_called()
#     mock_write.mode.assert_called()
#     mock_write.save.assert_called()
    
#     # Verify DeltaTable operations
#     mock_delta_table_class.isDeltaTable.assert_called()
#     mock_delta_table_class.forPath.assert_called()
#     mock_merge.execute.assert_called()
    
#     # Verify print statements
#     print_calls = [str(call) for call in mock_print.call_args_list]
#     assert any("Preferences joined with profiles:" in call for call in print_calls)
#     assert any("Appended records to" in call for call in print_calls)
#     assert any("Preferences data successfully merged into" in call for call in print_calls)


# def test_business_logic_overwrite_scenario():
#     """Test business logic when DeltaTable doesn't exist (overwrite scenario)"""
#     # Mock DataFrame behavior
#     mock_df = MagicMock()
#     mock_df.select.return_value = mock_df
#     mock_df.filter.return_value = mock_df
#     mock_df.alias.return_value = mock_df
#     mock_df.join.return_value = mock_df
#     mock_df.distinct.return_value = mock_df
#     mock_df.count.return_value = 42
    
#     # Mock write operations
#     mock_write = MagicMock()
#     mock_df.write = mock_write
#     mock_write.format.return_value = mock_write
#     mock_write.mode.return_value = mock_write
#     mock_write.save.return_value = None
    
#     # Mock spark session
#     mock_spark = MagicMock()
#     mock_read = MagicMock()
#     mock_spark.read = mock_read
#     mock_read.format.return_value = mock_read
#     mock_read.option.return_value = mock_read
#     mock_read.load.return_value = mock_df
    
#     # Mock DeltaTable operations for overwrite scenario
#     mock_delta_table_class = MagicMock()
#     mock_delta_table_class.isDeltaTable.return_value = False  # Table doesn't exist
    
#     # Patch both the spark variable and DeltaTable class
#     with patch("guesthub.guest_preference.business_logic_guest_preference.spark", mock_spark), \
#          patch("guesthub.guest_preference.business_logic_guest_preference.DeltaTable", mock_delta_table_class), \
#          patch("builtins.print") as mock_print:
        
#         # Import the business logic module
#         import importlib
#         import sys
        
#         # Clear the module from cache if it exists
#         module_name = "guesthub.guest_preference.business_logic_guest_preference"
#         if module_name in sys.modules:
#             del sys.modules[module_name]
        
#         # Import the module (this will execute the top-level code)
#         import guesthub.guest_preference.business_logic_guest_preference
    
#     # Verify DeltaTable.isDeltaTable was called
#     mock_delta_table_class.isDeltaTable.assert_called()
    
#     # Verify that forPath was NOT called (since table doesn't exist)
#     mock_delta_table_class.forPath.assert_not_called()
    
#     # Verify print statements
#     print_calls = [str(call) for call in mock_print.call_args_list]
#     assert any("Master preferences table created at" in call for call in print_calls)