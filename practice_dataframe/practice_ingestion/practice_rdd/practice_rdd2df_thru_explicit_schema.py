from pyspark.sql import SparkSession, Row
import os
import yaml
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, IntegerType, StringType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.4') \
        .appName("DataFrame Through RDD") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    current_dir = os.path.abspath(os.path.dirname(__file__))  # current_dir = os.getcwd()
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    txn_fct_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/txn_fct.csv") \
        .filter(lambda record: "txn_id" not in record) \
        .map(lambda record: record.split("|")) \
        .map(lambda record: (record[0], record[1], record[2], record[3], record[4], record[5], record[6]))

    txn_fct_schema = StructType([
        StructField("txn_id", LongType(), False),
        StructField("created_time_str", LongType(), False),
        StructField("amount", DoubleType(), True),
        StructField("cust_id", LongType(), True),
        StructField("status", IntegerType(), True),
        StructField("merchant_id", LongType(), True),
        StructField("created_time_ist", StringType(), True)
    ])

    txn_fct_df = spark.createDataFrame(txn_fct_rdd, txn_fct_schema)
    txn_fct_df.printSchema()
    txn_fct_df.show(5, False)

    # spark-submit --master yarn --packages "org.apache.hadoop:hadoop-aws:2.7.4" practice_dataframe/practice_ingestion/practice_rdd/practice_rdd2df_thru_explicit_schema.py
