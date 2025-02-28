# I have 2 doubt regarding currency conversions
# In Transaction table - we have Sender Name, Receiver Name, Country and Currency.
# I think it must be Sender Country and Sender Currency.
# From Receiver_Registry table :- We can get the receiver country only. But how would we know Receiver Currency ?
# without knowing Receiver Currency - how would we implement Currency-conversion feature.
# I apologize for inserting a new column ("receiver_currency") in Transaction table.

# Also the calculation logic for final report. I could not understand completely.
# especially how we are calculating sum and average class level
# I assumed - amount * percentage * currency_rate : would be calculated
#           _ sum it and get Average from above calculation
#           - But for class type C and D , target Excel is not matching with this assumption

import os
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc, col, row_number, explode, from_json, col, when, sum
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from Utils import *

logger = get_logger()


def calculate_guarantor_amount(trn_path, rec_registry_path, exg_rate_path):
    try:
        # Creating a Spark session
        create_spark_session()
        logger.info("Spark Session created:")

        transaction_df = read_csv_dataset(trn_path)
        receiver_registry_df = read_csv_dataset(rec_registry_path)
        exchange_df = read_csv_dataset(exg_rate_path)
        logger.info("All input dataframe are created")

        # Filtering only Active Transaction dataset
        transaction_filtered_df = transaction_df \
            .filter("status != 'CLOSED'") \
            .filter("date like '%2024%'")
        # TODO - this logic to filter inactive date - I am bit confused, so implemented as 2024 are only active
        logger.info("Transaction dataset is applied with all filters")

        # removing Outdated receiver list by applying a window function - row-number over partition_date
        windows_spec = Window.partitionBy("receiver_name").orderBy(desc("partition_date"))
        receiver_registry_filtered_df = receiver_registry_df \
            .withColumn("row_num", row_number().over(windows_spec)).filter("row_num = 1").drop("row_num")

        # removing Outdated Exchange rate list by applying a window function - row-number over partition_date
        windows_spec = Window.partitionBy("from_currency", "to_currency").orderBy(desc("partition_date"))
        exchange_filtered_df = exchange_df \
            .withColumn("row_num", row_number().over(windows_spec)) \
            .filter("row_num = 1") \
            .drop("row_num")

        # Verifying Transactions receiver details with Receiver Registry dataset
        transaction_valid_df = transaction_filtered_df \
            .join(broadcast(receiver_registry_df.select("RECEIVER_NAME")), on="RECEIVER_NAME", how="inner")

        # Removing duplicate records by transaction_id and partition_date
        windows_spec = Window.partitionBy("transaction_id").orderBy(desc("partition_date"))
        transaction_valid_df = transaction_valid_df \
            .withColumn("row_num", row_number().over(windows_spec)) \
            .filter("row_num = 1") \
            .drop("row_num")
        logger.info("duplicate records are removed from transaction table")
        # transaction_valid_df.show(20, False)

        # Applying Json schema and exploding JSON column - GUARANTORS to list of record
        guarantor_json_schema = ArrayType(StructType([
            StructField("NAME", StringType(), True),
            StructField("PERCENTAGE", DoubleType(), True)
        ]))

        transaction_json_df = transaction_valid_df \
            .withColumn("guarantor_objects", from_json(col("GUARANTORS"), guarantor_json_schema))

        transaction_exploded_df = transaction_json_df \
            .withColumn("guarantor_object", explode("guarantor_objects")) \
            .select([col for col in transaction_json_df.columns if
                     col not in ("guarantor_objects", "guarantor_object", "GUARANTORS")] + ["guarantor_object.*"]
                    )
        logger.info("Transaction dataset is exploded and selected only required columns")
        transaction_exploded_df.cache()
        # transaction_exploded_df.show(20, False)

        # Joining Transaction with Exchange Rate dataset and calculating amount based on currency_rate and percentage
        transaction_joined_df = transaction_exploded_df \
            .join(exchange_filtered_df,
                  (col("CURRENCY") == col("FROM_CURRENCY")) & (col("RECEIVER_CURRENCY") == col("TO_CURRENCY"))
                  ) \
            .withColumn("amount_computed", col("amount") * col("RATE") * col("percentage"))
        logger.info("transaction dataset joined with exchange data and amount calculated")
        # transaction_joined_df.show(20, False)

        # Calculating total amount for each class and it's average
        expected_classes = ["CLASS_A", "CLASS_B", "CLASS_C", "CLASS_D"]

        windows_spec = Window.partitionBy("NAME", "TYPE", "COUNTRY").orderBy()
        transaction_computed_df = transaction_joined_df \
            .select("NAME", "COUNTRY", "TYPE", "amount_computed", transaction_exploded_df["partition_date"])

        # Creating a new column for each class total and it's average
        for class_type in expected_classes:
            transaction_computed_df = transaction_computed_df.withColumn(
                class_type,
                F.sum(when(col("TYPE") == class_type, col("amount_computed")).otherwise(0)).over(windows_spec)
            ).withColumn(
                f"AVG_{class_type}",
                F.avg(when(col("TYPE") == class_type, col("amount_computed")).otherwise(0)).over(windows_spec)
            )
        logger.info("Class wise transaction amount is derived")
        # transaction_computed_df.show(20, False)

        # Removing duplicates from the dataset.
        windows_spec_remove_dups = Window.partitionBy("NAME", "COUNTRY")  # .orderBy("amount_computed")
        final_df = transaction_computed_df
        for class_type in expected_classes:
            final_df = final_df.withColumn(
                f"{class_type}",
                F.max(col(class_type)).over(windows_spec_remove_dups)
            ).withColumn(
                f"AVG_{class_type}",
                F.max(col(f"AVG_{class_type}")).over(windows_spec_remove_dups)
            )
        # final_df.show(20, False)

        final_outcome = final_df \
            .withColumn("row_num", row_number().over(windows_spec_remove_dups.orderBy("amount_computed"))) \
            .filter("row_num = 1") \
            .drop("amount_computed", "row_num")
        logger.info("==== Final output is printing ====")
        final_outcome.show(20, False)
    except Exception as e:
        logger.error(f"Error occurred - {e}")
        raise e


def main():
    # Reading all input datasets to a Dataframe
    trn_path = create_dataset_path(get_property_value("trn_path"))
    exg_rate_path = create_dataset_path(get_property_value("exg_rate_path"))
    rec_registry_path = create_dataset_path(get_property_value("rec_registry_path"))

    calculate_guarantor_amount(trn_path=trn_path, exg_rate_path=exg_rate_path, rec_registry_path=rec_registry_path)


if __name__ == '__main__':
    main()
