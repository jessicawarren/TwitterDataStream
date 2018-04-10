import sys
import sqlite3
from twython import Twython, TwythonError
from twython import TwythonStreamer
from pprint import pprint
import pymysql
from datetime import datetime
import pytz

table_name = str(sys.argv[1])
search_query = str(sys.argv[2])

def surround(string):
    return "\"" + string + "\""



class MyStreamer(TwythonStreamer):
    def on_success(self, tweet):
        keys = ['text', 'created_at', 'user']

        if all(key in tweet for key in keys):
            print(tweet["text"])
            try:
                query = "INSERT INTO {} VALUES (%s, %s, %s, %s, %s)".format(table_name)
                print(str(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')))
                cur.execute(
                    query,
                    (
                        tweet['user']['screen_name'],
                        str(datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')),
                        tweet['retweet_count'],
                        tweet['favorite_count'],
                        tweet['text']
                    )
                )
                connection.commit()

                #query = "INSERT INTO {} VALUES ({},{},{},{})".\
                #format(
                #    table_name,
                #    surround(tweet['user']["screen_name"]),
                #    #surround(tweet["created_at"]),
                #    tweet["retweet_count"],
                #    tweet["favorite_count"],
                #    surround(repr(tweet["text"])[1:-1])  # for a string s, if we do repr(s)[1:-1], it will escape quotes within the string
                #).encode('utf-8')
                #print(query)
                #cur.execute(queryw  c)
            except pymysql.err.InternalError as e:
                print(e)
                with open("error.txt", 'a') as out:
                    out.write(str(e) + "\n*******\n")


#, date DATETIME
table_schema = "CREATE TABLE IF NOT EXISTS {} (username TEXT, date DATETIME, retweets INTEGER, favorites INTEGER, tweet_text TEXT);".format(table_name)

#connection = sqlite3.connect("{}.db".format(db_name)) #twitters_tea
connection = pymysql.connect(host='localhost',
                             user='jessica',
                             password='Comp#0325',
                             db='',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

#"{}.db".format(db_name)) #twitters_tea

#
connection.text_factory = str #this is so it uses the python str class to create strings

cur = connection.cursor() #cur bc cursor is a class and you do not want to confict with keyword names
cur.execute('CREATE database IF NOT EXISTS `Tweets`;')
cur.execute('use Tweets;')
cur.execute(table_schema)

CONSUMER_KEY = 'CqloCfH9Z2k6rQPjE1kjFw1kd'
CONSUMER_SECRET = 'Up2yvaqpcDuqiofIQFusN1GUgr0b1Vc7FHyU36hijwPb4XHVD1'
ACCESS_KEY = '769213509045542913-mLtrkguM5XajJwB6JqNBFRdYPvHhWKE'
ACCESS_SECRET = '9Ut2ORBqRdcufsU405gXaZauawp4rhr1nn20sleeUY7uW'

t = MyStreamer(
    CONSUMER_KEY,
    CONSUMER_SECRET,
    ACCESS_KEY,
    ACCESS_SECRET
)

while True:
    try:
        t.statuses.filter(track=search_query)
    except:
        continue

