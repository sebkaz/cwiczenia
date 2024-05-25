
## LOAD SPARK SESSION object

SERVER = "broker:9092"

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
        
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "transactions")
        .load()
    )
    
    query =  (
        raw.writeStream
        .outputMode("append")
        .option("truncate", "false")
        .format("console")
        .start()
    )
    
    
    query.awaitTermination()
    query.stop()
