from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, MapType, DoubleType
from pyspark.sql.functions import from_json, col, date_format, current_timestamp, length, when, upper

spark = SparkSession.builder \
    .appName("K8sLogsAnalysisJob") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.2") \
    .getOrCreate()

# Kafka 설정
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka.kafka.svc.cluster.local:9092") \
    .option("subscribe", "k8s-logs") \
    .load()

# 기본 스키마
schema = StructType() \
    .add("log", StringType()) \
    .add("stream", StringType()) \
    .add("kubernetes", MapType(StringType(), StringType()))

# 가공 및 분석 로직 추가
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

analysis_df = parsed_df.withColumn("@timestamp", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
    .withColumn("log_length", length(col("log"))) \
    .withColumn("log_level", 
        when(col("log").contains("ERROR") | col("log").contains("error") | col("log").contains("Fail"), "ERROR")
        .when(col("log").contains("WARN") | col("log").contains("warn"), "WARN")
        .otherwise("INFO")) \
    .withColumn("pod_name", col("kubernetes")["pod_name"])

# 가공된 데이터를 새로운 인덱스로 전송
query = analysis_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch-svc.logging.svc.cluster.local") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true") \
    .option("es.index.auto.create", "true") \
    .option("checkpointLocation", "/tmp/spark-checkpoint-analysis-v1") \
    .start("k8s-logs-analysis")

query.awaitTermination()
