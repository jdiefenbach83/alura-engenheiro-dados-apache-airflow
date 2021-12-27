from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName('twitter_insight_tweet')\
        .getOrCreate()

    tweet = spark.read.json('/home/jefferson/datapipeline/datalake/silver/twitter_aluraonline/tweet')

    alura = tweet\
        .where("author_id = '1566580880'")\
        .select("author_id", "conversation_id")

    tweet = tweet.alias("tweet")\
        .join(
            alura.alias("alura"),
            [
                (f.col("alura.author_id") != f.col("tweet.author_id")) &
                (f.col("alura.conversation_id") == f.col("tweet.conversation_id"))
            ],
            "left"
        )\
        .withColumn(
            "alura_conversation",
            f.when(f.col("alura.conversation_id").isNotNull(), 1).otherwise(0)
        )\
        .withColumn(
            "reply_alura",
            f.when(f.col("tweet.in_reply_to_user_id") == '1566580880', 1).otherwise(0)
        )\
        .groupBy(f.to_date("created_at").alias("created_date"))\
        .agg(
            f.countDistinct("id").alias("n_tweets"),
            f.countDistinct("tweet.conversation_id").alias("n_conversation"),
            f.sum("alura_conversation").alias("alura_conversation"),
            f.sum("reply_alura").alias("reply_alura")
        )\
        .withColumn("weekday", f.date_format("created_date", "E"))

    tweet.coalesce(1)\
        .write\
        .json("/home/jefferson/datapipeline/datalake/gold/twitter_insight_tweet.json")
    