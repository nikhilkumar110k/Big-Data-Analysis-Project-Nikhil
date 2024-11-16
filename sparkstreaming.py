import socket
import tweepy
import json
import time
import keyboard
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


consumer_key = 'SvKhuf7ZFEFgEFx1R5SUVQn5r'
consumer_secret = 'beQXKhQAgYQWlaZrd3pGrvp9JdX4H9RqFfy8U8uJNAmAWTBzez'
access_token = '1774429511393882112-crl3vLbYurQUTy4Qsv2yjjoPu8jg9x'
access_secret = 'YgWUaXGDmahZV8uoIRenZVnzAw3MzKLbpXuiqgCTLrFf0'
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAOpwwwEAAAAABvUinglAFPJ5889RnnyDa9m9PCU%3DLeswCkysTDRXvRpMPo7uIBgjFrUARPnXNejXTi1bLg0JwIpQAj'


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

spark = SparkSession.builder.master("local").appName("CompanyTweets").getOrCreate()

def get_tweets(keywords, tweet_limit=100):
    client = tweepy.Client(bearer_token=BEARER_TOKEN)
    
    tweet_list = []
    
    while True:
        if keyboard.is_pressed('q'): 
            print("Exiting the program...")
            break  

        try:
            for keyword in keywords:
                response = client.search_recent_tweets(query=keyword, max_results=10)
                if response.data:
                    for tweet in response.data:
                        tweet_list.append({'tweet': tweet.text, 'created_at': tweet.created_at, 'company': keyword})
                
                tweet_list = tweet_list[-tweet_limit:]

                print(f"\nFetching tweets for companies: {keywords}")
                for tweet in tweet_list:
                    print(f"- {tweet['tweet']}")

                tweet_df = spark.createDataFrame(tweet_list)
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
