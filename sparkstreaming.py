import socket
import tweepy
from tweepy import OAuthHandler,StreamingClient,StreamResponse,streaming, OAuth2BearerHandler
import json

consumer_key='WTMzaTNPX05kVDVGNUxocTJDTC06MTpjaQ'
consumer_secret='BOdmgMH4wWNnzig19G02BxoP7UIIBJ_fo7SCjvc3oWeblI6azk'
access_token='1774429511393882112-crl3vLbYurQUTy4Qsv2yjjoPu8jg9x'
access_secret='YgWUaXGDmahZV8uoIRenZVnzAw3MzKLbpXuiqgCTLrFf0'
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAAOpwwwEAAAAABvUinglAFPJ5889RnnyDa9m9PCU%3DLeswCkysTDRXvRpMPo7uIBgjFrUARPnXNejXTi1bLg0JwIpQAj'  

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

