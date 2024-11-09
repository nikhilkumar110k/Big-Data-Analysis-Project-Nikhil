import socket
import tweepy
from tweepy import OAuthHandler,StreamingClient,StreamResponse,streaming, OAuth2BearerHandler
import json

consumer_key='177442951139388211x'
consumer_secret='Yg3MzKLbpXuiqgCTLrFf0'
access_token='177442ToPu8jg9x'
access_secret='YgWUpXuiqgCTLrFf0'
BEARER_TOKEN = 'AAAAAANejXTi1bLg0JwIpQAj'  

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


class TweetListener(tweepy.StreamingClient):
    def __init__(self, csocket, bearer_token):
        super().__init__(bearer_token)
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error:", e)
        return True

    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
  auth=OAuthHandler(consumer_key,consumer_secret)
  auth.set_access_token(access_token,access_secret)
  listener = TweetListener(c_socket, BEARER_TOKEN)
  existing_rules = listener.get_rules().data
  if existing_rules:
      listener.delete_rules([rule.id for rule in existing_rules])
  listener.add_rules(tweepy.StreamRule("Python"))
  listener.filter()

if __name__ =='__main__':
  s=socket.socket()
  host='127.0.0.1'
  port=5555
  s.bind((host,port))
  print('listening on port',port)
  s.listen(5)
  c,addr=s.accept()
  sendData(c)

