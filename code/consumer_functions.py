import json                  # for parsing JSON
import requests              # for working with HTTP
import cytoolz.curried as tz # functional programming library
import os                    # for using environment variables
import twitter               # for connecting to twitter API, python twitter tools
import numpy as np           # for matrix math
import pandas as pd          # for data wrangling
import sqlalchemy as sqlal   # for connecting to databases
import unicodecsv as csv     # for saving to CSV in utf-8 by default
import gzip                  # for compression of CSV output
import time                  # for simple benchmarks
import datetime as dt        # for converting the formats of timestamps
import pdb                   # for testing
import argparse              # for accepting command line arguments
# import sqlite3               # for interacting with SQLite databases

## Accept arguments
parser = argparse.ArgumentParser(description="Save a WordPress or Twitter stream")
parser.add_argument('stream_key', metavar='stream_key', type=str, nargs=1, 
                    help='Which stream to consume (tweets, likes, posts, or comments)')
STREAM_KEY = parser.parse_args().stream_key[0]

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
# ENDPOINT = 'sqlite'
ENDPOINT = 'csv_gz'
MODE = 'debug' # makes it less efficient, but more debug-able
# MODE = 'production'

## Primary Functions
def main():
    """Overall function to start it off"""
    print("Starting a stream consumer for {}".format(STREAM_KEY))
    connect_to_stream(STREAM_KEY)

def connect_to_stream(stream_key):
    """Connect to the appropriate stream"""
    if ENDPOINT == 'sqlite':
        saveing_function = save_sqlite
    else:
        saveing_function = save_csv_gz
    if stream_key == "tweets":
        connect_to_twitter_stream('tweets', saveing_function)
    else: # WordPress
        connect_to_wordpress_stream(stream_key, saveing_function)
    return True

def connect_to_wordpress_stream(stream_key, saveing_function):
    """Connect & consume a WordPress event stream"""
    # Connect to Stream
    r = requests.get(STREAM_URLS[stream_key], stream=True)
    lines = r.iter_lines()

    # Filter and Parse
    parse_functions = {
        'posts': parse_post,
        'likes': parse_like,
        'comments': parse_comment}
    stream = tz.pipe(
        lines,
        ## Parse
        tz.map(permissive_json_load), # parse the JSON, or return an empty dictionary
        tz.map(parse_functions[stream_key]), # returns a flat dictionary
    )

    # Collect
    saveing_function(stream_key, stream)

    return True

def connect_to_twitter_stream(stream_key, saveing_function):
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
    saveing_function(stream_key, twitter_stream)

    return True

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

## Filter Functions
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
    def reformat_timestamp(given_ts):
        """Reformat into WordPress.com format"""
        # Twitter example: "Sat Oct 10 14:48:34 +0000 2015"
        # WordPress example: "2015-10-10T19:42:34Z"
        return tz.pipe(
            given_ts,
            lambda x: dt.datetime.strptime(x, "%a %b %d %H:%M:%S +0000 %Y"),
            lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
    gv = get_value_if_present_nested(given_dict)
    return {
        'timestamp_ms': gv(['timestamp_ms']),
        'created_at': reformat_timestamp(gv(['created_at'])),
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

def parse_post(given_dict):
    """Return parsed subset of a post object"""
    def get_tags(given_dict):
        return tz.pipe(
            tz.get_in(['object', 'tags'], given_dict, default = []),
            tz.filter(lambda x: tz.get_in(['objectType'], x, default=None) == 'tag'),
            tz.map(lambda x: tz.get_in(['displayName'], x, default=None)),
            lambda x: ", ".join(x)
        )
    def get_categories(given_dict):
        return tz.pipe(
            tz.get_in(['object', 'tags'], given_dict, default = []),
            tz.filter(lambda x: tz.get_in(['objectType'], x, default=None) == 'category'),
            tz.map(lambda x: tz.get_in(['displayName'], x, default=None)),
            lambda x: ", ".join(x)
        )
    gv = get_value_if_present_nested(given_dict)
    return {
        'verb': gv(['verb']),
        'published': gv(['object', 'published']), # date-time stamp
        'objectType': gv(['object', 'objectType']),
        'displayName': gv(['displayName']), # title
        'permalinkUrl': gv(['object', 'permalinkUrl']),
        'summary': gv(['object', 'summary']),
        'content': gv(['object', 'content']), # includes some HTML markup
        'content_len': len_or_none(gv(['object', 'content'])),
        'tags': get_tags(given_dict),
        'categories': get_categories(given_dict),
        'actor_name': gv(['actor', 'displayName']),
        'actor_id': gv(['actor', 'id']),
        'actor_type': gv(['actor', 'objectType'])}

def parse_comment(given_dict):
    """Return a somewhat parsed subset of a comment object"""
    gv = get_value_if_present_nested(given_dict)
    return {
        'verb': gv(['verb']),
        'published': gv(['published']), # a date-time
        'objectType': gv(['object', 'objectType']),
        'url': gv(['object', 'url']),
        'id': gv(['object', 'id']),
        'content': gv(['content']),
        'content_len': len_or_none(gv(['content'])),
        'target_lang': gv(['target', 'lang']),
        'target_summary': gv(['target', 'summary']),
        'target_wpCommentCount': gv(['target', 'wpCommentCount']),
        'actor_name': gv(['actor', 'displayName']),
        'actor_id': gv(['actor', 'id']),
        'actor_type': gv(['actor', 'objectType']),
    }

def parse_like(given_dict):
    """Return a somewhat parsed subset of a like object"""
    gv = get_value_if_present_nested(given_dict)
    return {
        'verb': gv(['verb']),
        'published': gv(['published']),
        'objectType': gv(['object', 'objectType']),
        'url': gv(['object', 'url']),
        'displayName': gv(['object', 'displayName']),
        'target_name': gv(['target', 'displayName']),
        'target_objectType': gv(['target', 'objectType']),
        'actor_name': gv(['actor', 'displayName']),
        'actor_id': gv(['actor', 'wpcom:user_id']),
        'actor_type': gv(['actor', 'objectType']),
    }

## Saving Functions
def save_first(stream_key, stream_iterator):
    """Save the first entry in the stream as an example"""
    def parse_entry(given_entry):
        """parse either the WordPress stream strings, 
        or the semi-parsed twitter stream"""
        if isinstance(given_entry, str):
            return json.loads(given_entry)
        else:
            return dict(given_entry)
    file_name = '../data/samples/example_{}.json'.format(stream_key)
    with open(file_name, 'w') as outfile:
        tz.pipe( 
            next(stream_iterator), # first entry
            parse_entry,           # parse
            json.dumps,            # unparse
            outfile.write)         # save
    print("Saved {}".format(file_name))
    return True

def save_sqlite(stream_key, stream_iterator):
    """Save the given stream to a database, such as SQLite."""
    db_engine = sqlal.create_engine(
            'sqlite:///{}'.format(get_save_location(stream_key, '.sqlite')))
    save_size = {'debug':10, 'production': 1000}
    stored_stream = []
    for num, row in enumerate(stream_iterator):
        stored_stream.append(row)
        if (num % save_size[MODE]) == 0 and num != 0: # occasionally save to disc
            stored_frame = pd.DataFrame(stored_stream)
            stored_frame.to_sql('stream', db_engine, if_exists='append', index=False)
            stored_stream = []
            print_update(stream_key, num)   # feedback for debugging
    return True

def save_csv_gz(stream_key, stream_iterator):
    """Save the given stream to a compressed CSV file
    
    This is presently designed to write the rows in chunks
    rather than row by row. My thought was that if there was 
    overhead to each write, this chunking would reduce the number 
    of times the program needed to deal with that."""
    file_name = get_save_location(
        stream_key, 
        "_{}.csv.gz".format(time.strftime("%Y-%m-%d_%I-%M-%S"))) # timestamp, to prevent overwriting
    save_size = {'debug':10, 'production': 1000}
    with gzip.open(file_name, "w") as f :
        stored_stream = []
        for num, row in enumerate(stream_iterator):
            if num == 0:
                writer = csv.DictWriter(f, fieldnames=row.keys())
                writer.writeheader()
            if (num % save_size[MODE]) == 0 and num != 0:
                writer.writerows(stored_stream) # save stored stream entries
                stored_stream = []              # reset storage
                print_update(stream_key, num)   # feedback for debugging
            stored_stream.append(row)
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
    #TODO: consider replacing with: 
    # tz.get_in(key_list, given_dict, default=None)
    # rename function to: get_in_reordered
    return reduce(get_value_if_present, key_list, given_dict)

def len_or_none(given_item):
    """If it has one, return length, otherwise return None"""
    try: 
        return len(given_item)
    except:
        return None

def get_save_location(stream_key, file_ending):
    return "../data/{}_stream{}".format(stream_key, file_ending)

def print_and_pass(given_item):
    """A print function to peak in on streams"""
    print(given_item)
    return given_item

def print_update(stream_key, num):
    """Print a small update on how things are going"""
    print("{} {} {}".format(
        stream_key.ljust(8), 
        dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        num))

def permissive_json_load(given_item):
    """A version of json.load that returns an empty dictionary if
    the given_item can't be decoded"""
    try:
        return json.loads(given_item)
    except:
        return {}

if __name__ == '__main__':
    main()
