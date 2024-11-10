import socket
import tweepy
import json
import time
import keyboard  

consumer_key = 'SvKhuf7ZFEFgEFx1R5SUVQn5r'
consumer_secret = 'beQXKhQAgYQWlaZrd3pGrvp9JdX4H9RqFfy8U8uJNAmAWTBzez'
access_token = '1774429511393882112-crl3vLbYurQUTy4Qsv2yjjoPu8jg9x'
access_secret = 'YgWUaXGDmahZV8uoIRenZVnzAw3MzKLbpXuiqgCTLrFf0'
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAOpwwwEAAAAABvUinglAFPJ5889RnnyDa9m9PCU%3DLeswCkysTDRXvRpMPo7uIBgjFrUARPnXNejXTi1bLg0JwIpQAj'  

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

def get_tweets(keyword, tweet_limit=20):
    client = tweepy.Client(bearer_token=BEARER_TOKEN)
    
    tweet_list = []
    
    while True:
        if keyboard.is_pressed('q'): 
            print("Exiting the program...")
            break  
        
        try:
            response = client.search_recent_tweets(query=keyword, max_results=10)
            if response.data:
                for tweet in response.data:
                    tweet_list.append(tweet.text)
                
                tweet_list = tweet_list[-tweet_limit:]

                print(f"\nShowing the last {tweet_limit} tweets for keyword '{keyword}':\n")
                for tweet in tweet_list:
                    print(f"- {tweet}")
            else:
                print("No tweets found for this keyword.")
        
        except tweepy.errors.TooManyRequests as e:
            print("Rate limit reached. Sleeping for 15 minutes...")
            time.sleep(900)  
        time.sleep(5)  

if __name__ == '__main__':
    keyword = input("Enter a keyword to search for tweets: ")
    get_tweets(keyword, tweet_limit=20) 
