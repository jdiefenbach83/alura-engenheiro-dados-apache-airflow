from os.path import join
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Row

def get_result_count(df):
    return df\
        .select("meta.result_count")\
        .collect()[0]\
        .result_count

def get_tweets_data(df):
    return df\
        .select(
            f.explode("data").alias("tweets")
        ).select(
            "tweets.author_id", 
            "tweets.conversation_id", 
            "tweets.created_at", 
            "tweets.id", 
            "tweets.in_reply_to_user_id", 
            "tweets.public_metrics.*", 
            "tweets.text"
        )

def get_users_data(df):
    return df\
        .select(
            f.explode("includes.users").alias("users")
        ).select(
            "users.*"
        )

def export_json(df, destination):
    df.coalesce(1).write.mode('overwrite').json(destination)

def twitter_transform(spark, src, dst, process_date):
    df = spark.read.json(src)
    count = get_result_count(df)

    if count == 0:
        return

    tweet_df = get_tweets_data(df)
    user_df = get_users_data(df)

    table_dst = join(dst, "{table_name}", f"process_date={process_date}")

    export_json(tweet_df, table_dst.format(table_name='tweet'))
    export_json(user_df, table_dst.format(table_name='user'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
       description='Spark Twitter Transformation'
    )
    parser.add_argument('--src', required=True)
    parser.add_argument('--dst', required=True)
    parser.add_argument('--process-date', required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_transform(spark, args.src, args.dst, args.process_date)
