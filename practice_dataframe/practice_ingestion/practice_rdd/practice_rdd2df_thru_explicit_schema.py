from pyspark.sql import SparkSession, Row
import os
import yaml
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, IntegerType, StringType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("DataFrame Through RDD") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    current_dir = os.path.abspath(os.path.dirname(__file__))  # current_dir = os.getcwd()
    app_config_path = os.path.abspath(current_dir + "/../../../" + "application.yml")
    app_secret_path = os.path.abspath(current_dir + "/../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)

    secrets = open(app_secret_path)
    app_secrets = yaml.load(app_secret_path, Loader=yaml.FullLoader)

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_conf["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_conf["s3_conf"]["secret_access_key"])

    txn_fct_rdd = spark.sparkContext.textFile("s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/txn_fct.csv") \
        .filter(lambda record: record.find("txn_id")) \
        .map(lambda record: record.split("|")) \
        .map(lambda record: Row(int(record[0]), int(record[1]), float(record[2]), int(record[3]), int(record[4]),
                                int(record[5]), record[6]))

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

    # spark-submit --master yarn --packages "org.apache.hadoop:hadoop-aws:3.2.4"
    # practice_dataframe/practice_ingestion/practice_rdd/practice_rdd2df_thru_explicit_schema.py
