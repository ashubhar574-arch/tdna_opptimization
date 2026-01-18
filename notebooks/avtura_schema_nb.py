# Databricks notebook source
avtura_schema=StructType([
    StructField('Baggage Offloading Finish', StringType(), True),
    StructField('Baggage Offloading Start', StringType(), True),
    StructField('cargo_door_closed', StringType(), True),
    StructField('Catering_Arrived', StringType(), True),
    StructField('Catering_Requested', StringType(), True),
    StructField('Departure - Boarding Held for Temp', StringType(), True),
    StructField('Departure - Temp for Boarding', StringType(), True),
    StructField('Departure - Temp on Arrival', StringType(), True),
    StructField('acdmId', StringType(), True),
    StructField('aircraftCallSign', StringType(), True),
    StructField('aircraftReg', StringType(), True),
    StructField('aircraftType', StringType(), True),
    StructField('airline', StringType(), True),
    StructField('at', StringType(), True),
    StructField('cancelled', BooleanType(), True),
    StructField('delay_code1', StringType(), True),
    StructField('delay_code2', StringType(), True),
    StructField('delay_code3', StringType(), True),
    StructField('delay_duration_1', StringType(), True),
    StructField('delay_duration_2', StringType(), True),
    StructField('delay_duration_3', StringType(), True),
    StructField('deleted', BooleanType(), True),
    StructField('destination', StringType(), True),
    StructField('diverted', BooleanType(), True),
    StructField('et', StringType(), True),
    StructField(
        'flightDataUpdateItem',
        ArrayType(
            StructType([
                StructField('name', StringType(), True),
                StructField(
                    'value',
                    StructType([
                        StructField('type', StringType(), True),
                        StructField('value', StringType(), True)
                    ]),
                    True
                )
            ]),
            True
        ),
        True
    ),
    StructField('flightNumber', StringType(), True),
    StructField('flightStatus', StringType(), True),
    StructField('flightStatusAD', StringType(), True),
    StructField('flightStatusCode', StringType(), True),
    StructField('flightType', StringType(), True),
    StructField('gate', StringType(), True),
    StructField('linkedFlight', StringType(), True),
    StructField('origin', StringType(), True),
    StructField('partial', BooleanType(), True),
    StructField('scheduleDate', StringType(), True),
    StructField('source', StringType(), True),
    StructField('sourceLocation', StringType(), True),
    StructField('st', StringType(), True),
    StructField('stand', StringType(), True),
    StructField('total_delay_duration', StringType(), True)
])
