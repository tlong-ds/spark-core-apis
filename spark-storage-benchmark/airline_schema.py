"""Shared explicit schema for the Kaggle Airline Delays flights.csv dataset."""

from pyspark.sql import types as T


AIRLINE_SCHEMA = T.StructType(
    [
        T.StructField("YEAR", T.IntegerType(), True),
        T.StructField("MONTH", T.IntegerType(), True),
        T.StructField("DAY", T.IntegerType(), True),
        T.StructField("DAY_OF_WEEK", T.IntegerType(), True),
        T.StructField("AIRLINE", T.StringType(), True),
        T.StructField("FLIGHT_NUMBER", T.IntegerType(), True),
        T.StructField("TAIL_NUMBER", T.StringType(), True),
        T.StructField("ORIGIN_AIRPORT", T.StringType(), True),
        T.StructField("DESTINATION_AIRPORT", T.StringType(), True),
        T.StructField("SCHEDULED_DEPARTURE", T.IntegerType(), True),
        T.StructField("DEPARTURE_TIME", T.IntegerType(), True),
        T.StructField("DEPARTURE_DELAY", T.IntegerType(), True),
        T.StructField("TAXI_OUT", T.IntegerType(), True),
        T.StructField("WHEELS_OFF", T.IntegerType(), True),
        T.StructField("SCHEDULED_TIME", T.IntegerType(), True),
        T.StructField("ELAPSED_TIME", T.IntegerType(), True),
        T.StructField("AIR_TIME", T.IntegerType(), True),
        T.StructField("DISTANCE", T.IntegerType(), True),
        T.StructField("WHEELS_ON", T.IntegerType(), True),
        T.StructField("TAXI_IN", T.IntegerType(), True),
        T.StructField("SCHEDULED_ARRIVAL", T.IntegerType(), True),
        T.StructField("ARRIVAL_TIME", T.IntegerType(), True),
        T.StructField("ARRIVAL_DELAY", T.IntegerType(), True),
        T.StructField("DIVERTED", T.IntegerType(), True),
        T.StructField("CANCELLED", T.IntegerType(), True),
        T.StructField("CANCELLATION_REASON", T.StringType(), True),
        T.StructField("AIR_SYSTEM_DELAY", T.IntegerType(), True),
        T.StructField("SECURITY_DELAY", T.IntegerType(), True),
        T.StructField("AIRLINE_DELAY", T.IntegerType(), True),
        T.StructField("LATE_AIRCRAFT_DELAY", T.IntegerType(), True),
        T.StructField("WEATHER_DELAY", T.IntegerType(), True),
    ]
)