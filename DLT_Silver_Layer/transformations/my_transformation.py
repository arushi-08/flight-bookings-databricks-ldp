import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


#### bookings is fact table, so we don't do upsert

@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format('delta')\
        .load('/Volumes/workspace/bronze/bronze_volume/bookings/data')
    return df

@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table('stage_bookings') # or dlt.read('stage_bookings')
    df = df.withColumn('amount', col('amount').cast('double') )\
        .withColumn('modified_date', current_timestamp() )\
        .withColumn('booking_date', to_date(col('booking_date')) ) \
        .drop('_rescued_data')
    return df

rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}


@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all(rules)
def silver_bookings():

    df = spark.readStream.table("trans_bookings")
    
    return df

#########################################################################
# flights - dim

@dlt.view(
    name = "trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/bronze/bronze_volume/flights/data/")
    df = df.withColumn('modified_date', current_timestamp() )\
        .drop('_rescued_data')

    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = col("flight_id"),
  stored_as_scd_type = 1
)


#########################################################################
# customers / passengers - dim

@dlt.view(
    name = "trans_customers"
)
def trans_customers():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/bronze/bronze_volume/customers/data/")
    df = df.withColumn('modified_date', current_timestamp() )\
        .drop('_rescued_data')

    return df

dlt.create_streaming_table("silver_customers")

dlt.create_auto_cdc_flow(
  target = "silver_customers",
  source = "trans_customers",
  keys = ["passenger_id"],
  sequence_by = col("passenger_id"),
  stored_as_scd_type = 1
)



#########################################################################
# airports - dim

@dlt.view(
    name = "trans_airports"
)
def trans_airports():
    df = spark.readStream.format("delta")\
            .load("/Volumes/workspace/bronze/bronze_volume/airports/data/")
    df = df.withColumn('modified_date', current_timestamp() )\
        .drop('_rescued_data')

    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
  target = "silver_airports",
  source = "trans_airports",
  keys = ["airport_id"],
  sequence_by = col("airport_id"),
  stored_as_scd_type = 1
)




