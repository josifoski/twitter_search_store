# Python script for storing twitter search
# Crated by Aleksandar Josifoski https://about.me/josifsk for Asma Mohammed
# 2017 February 28

import tweepy
import sys
import os
import codecs
import time
import html
import traceback
import logging

# In next list set desired search queries
li = ['#Trump', '@twitter']

# specify directory where script will be
# windows example 'D:\\data\\twitter_search_store\\'
# for Linux, MacOS like in following next lines
dir_in = '/data/git/twitter_search_store/'
# specify directory where output files will be
dir_out = '/data/twitter/'

# if log file become too heavy, you can change in next line filemode = 'w' which will create new log file, not appending.
logging.basicConfig(filename = dir_out + 'twitter_search_store.log', filemode = 'a', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

enablePrint = True
separator = '\t' # TAB key is choosen for separator

# note: when opening csv files in libreoffice use this options http://imgur.com/a/0jDnx

# Twitter authentication part
# Replace the API_KEY and API_SECRET with your application's key and secret. from http://apps.twitter.com
# put your API_KEY and API_SECRET in twitappkeys.dat file which you will place in dirapikeys directory
# specified one line bellow in first and second line
dirapikeys = '/data/twitter/'
with open(dirapikeys + 'twitappkeys.txt') as fkeys:
    lfkeys = fkeys.readlines()
    API_KEY = lfkeys[0].strip()
    API_SECRET = lfkeys[1].strip()
    
maxTweets = 10000 # You can set this number to what ever you like value, 
# but keep in mind that twitter keeps records for searches publicly available via API to up to last 7 days

tweetsPerQry = 100 # do not change this number

auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    logging.debug("Can't Authenticate")
    sys.exit(-1)

# Advice: don't change sinceId and max_id values
# If results from a specific ID onwards set since_id to that ID.
# else default to no lower limit sinceId = None, go as far back as API allows
sinceId = None
# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit max_id = -1, start from the most recent tweet matching the search query.
max_id = -1

# via this function you can put in csv file only some of the fields in desired order
# linclude = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] #here all fields are included.
linclude = [3, 11, 15]
def tinyreport(s):
    l = s.split(separator)
    tr = ''
    for cind in range(len(linclude)):
        c = linclude[cind]
        if cind < len(linclude) - 1:
            tr += l[c] + separator
        else:
            tr += l[c]
    return tr

# "id_str" is at position 0, "in_reply_to_status_id_str" is at position 1,
# and sequentially "source" is at position 14, "text" is at position 15
firstrow = \
    "id_str" + separator + \
    "in_reply_to_status_id_str" + separator + \
    "in_reply_to_screen_name" + separator + \
    "author.screen_name" + separator + \
    "author.name" + separator + \
    "author.id" + separator + \
    "author.description" + separator + \
    "author.created_at" + separator + \
    "author.location" + separator + \
    "author.friends_count" + separator + \
    "author.followers_count" + separator + \
    "created_at" + separator + \
    "retweet_count" + separator + \
    "favorite_count" + separator + \
    "source" + separator + \
    "text" + \
    os.linesep
    
def tweetInLine(tweetobj):
    line = \
    str(tweetobj.id_str) + separator + \
    str(tweetobj.in_reply_to_status_id_str) + separator + \
    str(tweetobj.in_reply_to_screen_name) + separator + \
    '@' + str(tweetobj.author.screen_name) + separator + \
    str(tweetobj.author.name).replace('\n', ' ')  + separator + \
    str(tweetobj.author.id) + separator + \
    str(tweetobj.author.description).replace('\n', ' ').replace('\r', '') + separator + \
    str(tweetobj.author.created_at) + separator + \
    str(tweetobj.author.location).replace('\n', ' ') + separator + \
    str(tweetobj.author.friends_count) + separator + \
    str(tweetobj.author.followers_count) + separator + \
    str(tweetobj.created_at) + separator + \
    str(tweetobj.retweet_count) + separator + \
    str(tweetobj.favorite_count) + separator + \
    str(tweetobj.source).replace('\n', ' ') + separator + \
    html.unescape(tweetobj.text.replace('\n', ' ').replace('\r', '').replace(separator, ' ')) + \
    os.linesep
    return tinyreport(line)
    
def writeIntweetsdb(tweetobj):
    global g
    g.write(tweetInLine(tweetobj))

time1 = time.time()

for searchQuery in li:
    searchQuery = searchQuery.strip()
    countnewtweets = 0

    #parsing only new tweets
    try:
        with open(dir_out + searchQuery.strip('@#') + 'id_history.txt') as fhis:
            x = fhis.read()
            x = x.strip()
            if x == '':
                lhis = []
            else:
                lhis = x.split('\n')
    except Exception as esc:
        lhis = []

    gName = searchQuery.strip('@#')
    if enablePrint: print('processing: ' + searchQuery)
    logging.debug('processing: ' + searchQuery)

    tweetCount = 0
    if enablePrint: print('-' * 40)

    with codecs.open(dir_out + gName + '_tweetsdb.csv', 'a', 'utf-8') as g:
        if len(lhis) == 0:
            g.write(tinyreport(firstrow))
            
        while tweetCount < maxTweets:
            try:
                if (max_id <= 0):
                    if (not sinceId):
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry)
                    else:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                since_id=sinceId)
                else:
                    if (not sinceId):
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1))
                    else:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1),
                                                since_id=sinceId)
                if not new_tweets:
                    if enablePrint: print("No more tweets found")
                    logging.debug("No more tweets found")
                    break

                for tweet in new_tweets:
                    if tweet.id_str not in lhis:
                        countnewtweets += 1
                        lhis.append(tweet.id_str)
                        # here we catched new tweet
                        try:
                            writeIntweetsdb(tweet)
                        except:
                            pass # sometime someone may post tweet, and later to delete same
                                 # and tweet to stay for some time period in search cache
                # convert lhis to history file
                with open(dir_out + searchQuery.strip('@#') + 'id_history.txt', 'w') as fhis:
                    for hisitem in lhis:
                        fhis.write(hisitem + os.linesep)
                
                tweetCount += len(new_tweets)
                if enablePrint: print("Downloaded %s tweets" % str(tweetCount).rjust(6))
                logging.debug("Downloaded %s tweets" % str(tweetCount).rjust(4))
                max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                print(str(e))
                logging.error(str(e))
                # next lines depends on imported traceback, they show line number in
                # code where eventual error may occure
                for frame in traceback.extract_tb(sys.exc_info()[2]):
                    fname, lineno, fn, text = frame
                    print("Error in %s on line %d" % (fname, lineno))
                    logging.error("Error in %s on line %d" % (fname, lineno))

    if enablePrint: print()

    time2 = time.time()
    hours = int((time2-time1)/3600)
    minutes = int((time2-time1 - hours * 3600)/60)
    sec = time2 - time1 - hours * 3600 - minutes * 60
    if enablePrint: print("tweets downloaded in %dh:%dm:%ds" % (hours, minutes, sec))
    if enablePrint: print()
    if enablePrint: print("new tweets: " + str(countnewtweets))
    logging.debug("new tweets: " + str(countnewtweets))
    time1 = time.time()
    if enablePrint: print()
    if enablePrint: print()

    sinceId = None
    max_id = -1
