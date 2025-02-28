import logging
import os
import configparser

from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('config.properties')
spark_session = None


def get_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        # filename="guarantor_transaction.log"
    )
    logger = logging.getLogger()
    return logger


def is_not_null_empty(path):
    return path and path is not None and len(path) > 0


def create_dataset_path(path):
    if is_not_null_empty(path):
        return os.path.abspath(path)
    else:
        raise "Invalid Path"


def get_property_value(prop):
    return config.get('DEFAULT', prop)


def create_spark_session():
    global spark_session
    spark_session = SparkSession.builder \
        .appName("GuarantorTransaction") \
        .getOrCreate()
    if spark_session:
        return spark_session


def read_csv_dataset(dataset_path):
    if is_not_null_empty(dataset_path) and spark_session:
        df = spark_session.read.option("header", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("inferSchema", True) \
            .csv(dataset_path)
        return df
    else:
        if not is_not_null_empty(dataset_path):
            raise "Invalid dataset path"
        else:
            raise "Invalid Spark Session"
