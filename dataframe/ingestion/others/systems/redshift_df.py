from pyspark.sql import SparkSession
import yaml
import os.path

if __name__ == '__main__':

    # os.environ["PYSPARK_SUBMIT_ARGS"] = (
    #     '--jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"\
    #      --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    # )

    def get_redshift_jdbc_url(redshift_config: dict):
        host = redshift_config["redshift_conf"]["host"]
        port = redshift_config["redshift_conf"]["port"]
        database = redshift_config["redshift_conf"]["database"]
        username = redshift_config["redshift_conf"]["username"]
        password = redshift_config["redshift_conf"]["password"]
        return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .config("spark.jars", "redshift-jdbc42-2.1.0.26")\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    print("Reading txn_fact table ingestion AWS Redshift and creating Dataframe,")

    # jdbc_url = get_redshift_jdbc_url(app_secret)
    # # jdbc_url = "jdbc:redshift://myredshiftcluster.590183684400.eu-west-1.redshift-serverless.amazonaws.com:5439/dev?user=admin&password=Admin1234"
    # print(jdbc_url)
    #
    # "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)
    # txn_df = spark.read\
    #     .format("io.github.spark_redshift_community.spark.redshift")\
    #     .option("url", jdbc_url) \
    #     .option("query", app_conf["redshift_conf"]["query"]) \
    #     .option("forward_spark_s3_credentials", "true")\
    #     .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp")\
    #     .load()

    url = "jdbc:redshift://myredshiftcluster.590183684400.eu-west-1.redshift-serverless.amazonaws.com:5439/dev?user=admin&password=Admin1234"
    aws_iam_role_arn = "arn:aws:iam::590183684400:role/service-role/AmazonRedshift-CommandsAccessRole-20240322T172227"

    tnx_df = spark.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url) \
        .option("dbtable", "public.txn_fct") \
        .option("tempdir", "s3://spark-faisal-spark/temp") \
        .option("aws_iam_role", aws_iam_role_arn) \
        .load()

    tnx_df.show(5, False)

    tnx_df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url) \
        .option("dbtable", "public.txn_fct_new") \
        .option("tempdir", "s3://spark-faisal-spark/temp") \
        .mode("overwrite") \
        .save()


# spark-submit  --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4" dataframe/ingestion/others/systems/redshift_df.py
# --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"


# Use this spark submit commant it will work
# spark-submit --packages "io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.spark:spark-avro_2.11:2.4.2,org.apache.hadoop:hadoop-aws:2.7.4"\
#   --jars "/usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar" \
#   dataframe/ingestion/others/systems/redshift_df.py