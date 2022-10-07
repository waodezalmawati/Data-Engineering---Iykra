from confluent_kafka import Producer
import tweepy
import time
# import json
from datetime import datetime
# import logging
# import random

#Twitter Setup
consumer_key = "WAk05r71QCiV6UoDBEWHB1UII"
consumer_secret = "Y2ICU8nHLMtNyQtLnV8MnIJStdEgQqJ31MHsYNwJQ8q1G0iDvw"
access_token = "1410741456352944130-V2uYaPRq1Ymyfe88038rwsMHTkEX37"
access_token_secret ="UOYOdQUNTWoG5rjNDJOCUv0bbQnVqL07YOocq9N0JHN60"

#Create authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
#Setting access token
auth.set_access_token(access_token, access_token_secret)
#Passing in auth information
api = tweepy.API(auth)

# logging.basicConfig(format='%(asctime)s %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S',
#                     filename='producer.log',
#                     filemode='w')

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

def normalize_timestamp(time):
    times = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (times.strftime("%Y-%m-%d %H:%M:%S"))

####################
p=Producer({'bootstrap.servers':'localhost:9092'})
print('Kafka Producer has been initiated...')

#####################
# def receipt(err,msg):
#     if err is not None:
#         print('Error: {}'.format(err))
#     else:
#         message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
#         logger.info(message)
#         print(message)
        
#####################
def get_twitter_data():
    data = api.search("kanjuruhan")
    for i in data:
        record = ''
        record += str(i.user.id_str)
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record += str(i.user.followers_count)
        record += ';'
        record += str(i.user.location)
        record += ';'
        record += str(i.text)
        record += ';'
        record += str(i.retweet_count)
        record += ';'
    p.produce('data_twitter', str.encode(record))
    time.sleep(3)

# get_twitter_data()
def periodic(interval) :
    while True:
        get_twitter_data()
        time.sleep(interval)
# periodic(60*0.1)
        
if __name__ == '__main__':
    get_twitter_data()
    periodic(60 * 0.1)