import socket
import tweepy
import json
import time
import keyboard
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


consumer_key = 'Sv'
consumer_secret = 'b'
access_token = '177'
access_secret = 'YgWUaXG'
BEARER_TOKEN = 'AAAAAAAAAAAAAj'


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

spark = SparkSession.builder.master("local").appName("CompanyTweets").getOrCreate()

def get_tweets(keywords, tweet_limit=100):
    client = tweepy.Client(bearer_token=BEARER_TOKEN)
    
    tweet_list = []

    # Define schema explicitly
    schema = StructType([
        StructField("tweet", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("company", StringType(), True)
    ])
    
    while True:
        if keyboard.is_pressed('n'): 
            print("Exiting the program...")
            break  

        try:
            for keyword in keywords:
                response = client.search_recent_tweets(query=keyword, max_results=100)
                if response.data:
                    for tweet in response.data:
                        tweet_list.append({
                            'tweet': tweet.text, 
                            'created_at': tweet.created_at, 
                            'company': keyword
                        })
                
                tweet_list = tweet_list[-tweet_limit:]

                print(f"\nFetching tweets for companies: {keywords}")
                for tweet in tweet_list:
                    print(f"- {tweet['tweet']}")

                # Create DataFrame with explicit schema
                tweet_df = spark.createDataFrame(tweet_list, schema=schema)
                tweet_df.show()

                tweet_df = tweet_df.withColumn('date', F.to_date(tweet_df['created_at']))
                company_count = tweet_df.groupBy('company', 'date').count().orderBy('date')

                company_count_pd = company_count.toPandas()
                
                plt.figure(figsize=(10, 6))
                for company in keywords:
                    company_data = company_count_pd[company_count_pd['company'] == company]
                    plt.plot(company_data['date'], company_data['count'], label=company)
                
                plt.xlabel('Date')
                plt.ylabel('Number of Tweets')
                plt.title('Number of Tweets per Company Over Time')
                plt.legend()
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.show()

            else:
                print("No tweets found for this keyword.")
        
        except tweepy.errors.TooManyRequests as e:
            print("Rate limit reached. Sleeping for 15 minutes...")
            time.sleep(900) 
        time.sleep(5) 

if __name__ == '__main__':
    companies = ['Tesla', 'Google', 'Apple', 'Microsoft']
    get_tweets(companies, tweet_limit=100)
