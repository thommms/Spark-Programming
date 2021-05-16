import tweepy
from tweepy import OAuthHandler # to authenticate Twitter API
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import time
import io

# Twitter developer Credentials to connect to twitter account
access_token = "###########################"
access_secret = "##########################"
consumer_key = "###########################"
consumer_secret = "##################################"

auth = OAuthHandler(consumer_key, consumer_secret) #OAuth object
auth.set_access_token(access_token, access_secret)

start_time = time.time() #grabs the system time
keyword_list = ['bitcoin','football'] #track list

time_limit_to_stream_data = 35 #this is equivalent to 30 minutes

#Listener Class Override
class listener(StreamListener):

        def __init__(self, start_time, time_limit=30): #the time here is in seconds

                self.time = start_time
                self.limit = time_limit
                self.tweet_data = []

        def on_data(self, data):
                saveFile = open('json_tweet_data.json', 'a', encoding='utf-8')  #this appends or adds to the json file created as the stream is on
                while (time.time() - self.time) < self.limit:
                        try:
                                self.tweet_data.append(data)
                                return True

                        except:
                                print ("failed ondata")
                                time.sleep(5)
                                pass

                saveFile = open('json_tweet_data.json', 'w', encoding='utf-8') #this writes to the json file and creates it if it does not exist
                saveFile.write(u'[\n')
                saveFile.write(','.join(self.tweet_data))
                saveFile.write(u'\n]')
                saveFile.close()
                exit()

        def on_error(self, status):
                print (status)

print("\n===================================== Streaming will start now=============================================================")
local_start_time = time.ctime(start_time) #converted the time function to a local readable time
print("================>Streaming starts at : " , local_start_time)
print("\nStreaming data from twitter, please wait...")
print("\n")
twitterStream = Stream(auth, listener(start_time, time_limit=time_limit_to_stream_data)) #initialize Stream object with a time out limit
twitterStream.filter(track=keyword_list, languages=['en'])  #call the filter method to run the Stream Object

print("\n=====================================Ending streaming....=============================================================")
end_time= time.time()
local_end_time = time.ctime(end_time) #converted the time function to a local readable time
print("================>Streaming ended at : " , local_end_time)
print("\n")

