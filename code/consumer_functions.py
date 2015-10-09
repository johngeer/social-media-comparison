import json                  # parsing JSON
import requests              # for working with HTTP
import cytoolz.curried as tz # functional programming library
import os                    # for using environment variables
import twitter               # python twitter tools
import numpy as np           # for matrix math
import pandas as pd          # for data wrangling
import sqlalchemy as sqlal   # for connecting to databases
import time                  # for simple benchmarks
import unicodecsv as csv     # for saving to csv in utf-8 by default
import gzip                  # for compression of csv or json
import pdb                   # for testing
# import sqlite3               # for interacting with sqlite databases

## Configuration
STREAM_URLS = {
    "likes": 'http://xmpp.wordpress.com:8008/likes.json?type=text/plain',
    "posts": 'http://xmpp.wordpress.com:8008/posts.json?type=text/plain',
    "comments": 'http://xmpp.wordpress.com:8008/comments.json?type=text/plain'
}
TWITTER_CREDENTIALS = {
    "access_token": os.environ['TWITTER_ACCESS_TOKEN'],
    "access_token_secret": os.environ['TWITTER_ACCESS_SECRET'],
    "consumer_key": os.environ['TWITTER_CONSUMER_KEY'],
    "consumer_secret": os.environ['TWITTER_CONSUMER_SECRET']
}

def connect_to_stream(stream_key):
    """Connect to the appropriate stream"""
    if stream_key == "twitter":
        connect_to_twitter_stream()
    else: # WordPress
        connect_to_wordpress_stream(stream_key)
    return True

def connect_to_wordpress_stream(stream_key):
    """Connect & consume a WordPress event stream"""
    r = requests.get(STREAM_URLS[stream_key], stream=True)
    lines = r.iter_lines()

    save_first(
        '../data/samples/example_{}.json'.format(stream_key), 
        lines)

    return True

def connect_to_twitter_stream():
    """Connect & consume a twitter stream"""
    # Connect to Stream
    auth = twitter.OAuth(
        consumer_key=TWITTER_CREDENTIALS['consumer_key'],
        consumer_secret=TWITTER_CREDENTIALS['consumer_secret'],
        token=TWITTER_CREDENTIALS['access_token'],
        token_secret=TWITTER_CREDENTIALS['access_token_secret']
    )
    twitter_public_stream = twitter.TwitterStream(auth=auth)

    # Filter and Parse
    twitter_stream = tz.pipe(
        twitter_public_stream.statuses.sample(), # raw stream
        ## Filter
        tz.filter(is_tweet), # filter to tweets
        # tz.filter(is_user_lang_tweet(["en", "en-AU", "en-au", "en-GB", "en-gb"])), # filter to English
        ## Parse
        tz.map(parse_tweet), # parse into flat dictionary
    )

    # Collect
    save_csv_gz(
        "../data/twitter_stream.csv.gz",
        twitter_stream)

    return True

def main():
    pass

## Decorators
def timed(func):
    """A decorator to print the execution time of a given function
    Helpful for simple benchmarking"""
    def new_func(*args, **kargs):
        start_time = time.time()
        result = func(*args, **kargs)
        print("The {} function took {}".format(func.__name__, time.time() - start_time))
        return result
    return new_func
## Filter functions
def is_tweet(given_item):
    """Predicate to check whether a item is a tweet / status update"""
    if set(given_item.keys()).isdisjoint([ 
            'delete', 
            'scrub_geo', 
            'limit', 
            'status_withheld', 
            'user_withheld', 
            'disconnect', 
            'warning', 
            'event']):
        return True
    else:
        return False
@tz.curry
def is_user_lang_tweet(allow_lang_list, given_item):
    """Predicate to check whether the user_lang of the given item is in the 
    given list of allowed languages"""
    user_lang = get_value_if_present_nested(given_item, ['user', 'lang'])
    return user_lang in allow_lang_list
## Parsing Functions
def parse_tweet(given_dict):
    """Reorganize the resulting dictionary"""
    def len_or_none(given_item):
        """Return length, if it has one. Otherwise None"""
        try: 
            return len(given_item)
        except:
            return None
    def get_hashtag_string(given_item):
        """Return a string of hashtags associated with the given item"""
        ht = get_value_if_present_nested(given_item, ['entities', 'hashtags'])
        if len(ht) > 0:
            return ", ".join(
                map(
                    lambda x: get_value_if_present_nested(x, ['text']), 
                    ht))
        else:
            return ''
    gv = get_value_if_present_nested(given_dict)
    return {
        'timestamp_ms': gv(['timestamp_ms']),
        'created_at': gv(['created_at']),
        'text': gv(['text']),
        'hashtags': get_hashtag_string(given_dict),
        'is_quote_status': gv(['is_quote_status']),
        'user_id': gv(['user','id']),
        'user_scree_name': gv(['user','screen_name']),
        'user_lang': gv(['user','lang']),
        'user_favourites': gv(['user','favourites_count']),
        'count_urls': len_or_none(gv(['entities','urls'])),
        'count_media': len_or_none(gv(['entities','media'])),
        'is_reply': gv(['in_reply_to_user_id']) is not None,
        'is_retweet': gv(['retweeted_status']) is not None, 
        'time_zone': gv(['user', 'time_zone'])}
def parse_post(given_dict): #TODO: test
    """Return parsed subset of a post object"""
    def get_tags(given_dict):
        return tz.pipe(
            get_value_if_present_nested(given_dict, ['object', 'tags']),
            tz.map(lambda x: tz.dissoc(x, 'url')), # remove url
            json.dumps
        )
    gv = get_value_if_present_nested(given_dict)
    return {
        'verb': gv(['verb']),
        'published': gv(['object', 'published']),
        'displayName': gv(['displayName']),
        'permalinkUrl': gv(['object', 'permalinkUrl']),
        'summary': gv(['object', 'summary']),
        # decided not to include the full content of the post
        'tags': get_tags(given_dict),
        'actor_name': gv(['actor', 'displayName']),
        'actor_id': gv(['actor', 'id']),
        'actor_type': gv(['actor', 'objectType'])}
def parse_comment(given_dict): #TODO
    """Return a somewhat parsed subset of a comment object"""
    pass
def parse_like(given_dict): #TODO
    """Return a somewhat parsed subset of a like object"""
    pass
## Saving functions
def save_first(file_name, stream_iterator):
    """Save the first entry in the stream as an example"""
    def parse_entry(given_entry):
        """parse either the WordPress stream strings, 
        or the semi-parsed twitter stream"""
        if isinstance(given_entry, str):
            return json.loads(given_entry)
        else:
            return dict(given_entry)
    with open(file_name, 'w') as outfile:
        tz.pipe( 
            next(stream_iterator), # first entry
            parse_entry,           # parse
            json.dumps,            # unparse
            outfile.write)         # save
    print("Saved {}".format(file_name))
    return True
def save_sqlite(db_engine, stream_iterator):
    """Save the given stream to a SQLite database"""
    stored_stream = []
    for num, row in enumerate(stream_iterator):
        stored_stream.append(row)
        if (num % 2000) == 0: # occasionally save to disc
            stored_frame = pd.DataFrame(stored_stream)
            stored_frame.to_sql('stream', db_engine, if_exists='append', index=False)
            stored_stream = []
            print(num)
    return True
def save_csv_gz(file_name, stream_iterator):
    """Save the given stream to a compressed CSV file"""
    file_name = file_name.replace( # ad timestamp, to prevent overwriting
        ".csv.gz", 
        "_{}.csv.gz".format(time.strftime("%Y-%m-%d_%I-%M-%S")))
    with gzip.open(file_name, "w") as f :
        for num, row in enumerate(stream_iterator):
            if num == 0:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                writer.writeheader()
            if (num % 2000) == 0:
                print(num)
            writer.writerow(row)
    return True

## Helper Functions
def get_value_if_present(given_dict, key_string):
    """Return the value associated with key_string, or None"""
    if isinstance(given_dict, dict) and key_string in given_dict.keys():
        return given_dict[key_string]
    else:
        return None
@tz.curry
def get_value_if_present_nested(given_dict, key_list):
    """Like value_if_present but can handle nesting"""
    #TODO: replace with: 
    # tz.get_in(key_list, given_dict, default=None)
    return reduce(get_value_if_present, key_list, given_dict)

if __name__ == '__main__':
    connect_to_twitter_stream()
    main()
