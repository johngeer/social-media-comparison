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
import yaml                  # for loading the configuration file
# import sqlite3               # for interacting with SQLite databases

## Accept Arguments
parser = argparse.ArgumentParser(description="Save a WordPress or Twitter stream")
parser.add_argument('stream_key', metavar='stream_key', type=str, nargs=1, 
                    help='Which stream to consume (tweets, likes, posts, or comments)')
STREAM_KEY = tz.get_in([0], parser.parse_args().stream_key, default=None)

## Load Configuration
with open('config.yaml') as config_file:
    CONFIG = yaml.load(config_file.read())
TWITTER_CREDENTIALS = {
    "access_token": os.environ['TWITTER_ACCESS_TOKEN'],
    "access_token_secret": os.environ['TWITTER_ACCESS_SECRET'],
    "consumer_key": os.environ['TWITTER_CONSUMER_KEY'],
    "consumer_secret": os.environ['TWITTER_CONSUMER_SECRET']
}

## Primary Functions
def main():
    """Overall function to start it off"""
    print("Starting a stream consumer for {}".format(STREAM_KEY))
    connect_to_stream(STREAM_KEY)

def connect_to_stream(stream_key):
    """Connect to the appropriate stream"""

    # Set save function
    if CONFIG['endpoint'] == 'sqlite':
        saveing_function = save_sqlite
    else:
        saveing_function = save_csv_gz

    # Pick which stream to save
    if stream_key == "filtered_tweets":
        connect_to_twitter_filtered_stream('filtered_tweets', saveing_function)
    elif stream_key == "tweets":
        connect_to_twitter_stream('tweets', saveing_function)
    else: # WordPress
        connect_to_wordpress_stream(stream_key, saveing_function)
    return True

def connect_to_wordpress_stream(stream_key, saveing_function):
    """Connect to & consume a WordPress event stream"""
    parse_functions = {
        'posts': parse_post,
        'likes': parse_like,
        'comments': parse_comment}
    stream = tz.pipe(
        ## Connect
        start_wordpress_stream(CONFIG['stream_urls'][stream_key]),
        ## Parse
        tz.map(permissive_json_load), # parse the JSON, or return an empty dictionary
        tz.map(parse_functions[stream_key]), # parse into a flat dictionary
    )

    # Collect
    saveing_function(stream_key, stream)

def connect_to_twitter_stream(stream_key, saveing_function):
    """Connect to & consume a Twitter stream"""
    stream = tz.pipe(
        ## Connect
        start_stream_twitter(), # public sampled stream
        tz.map(print_twitter_stall_warning),
        ## Filter
        tz.filter(is_tweet), # filter to tweets
        # tz.filter(is_user_lang_tweet(["en", "en-AU", "en-au", "en-GB", "en-gb"])), # filter to English
        ## Parse
        tz.map(parse_tweet), # parse into a flat dictionary
    )

    # Collect
    saveing_function(stream_key, stream)

def connect_to_twitter_filtered_stream(stream_key, saveing_function):
    """Connect to & consume a filtered Twitter stream, where Twitter does 
    some of the filtering"""
    stream = tz.pipe(
        ## Connect
        start_stream_twitter(**CONFIG['twitter_filter']),
        tz.map(print_twitter_stall_warning),
        ## Filter
        tz.filter(is_tweet), # filter to tweets
        ## Parse
        tz.map(parse_tweet), # parse into a flat dictionary
    )

    ## Collect
    saveing_function(stream_key, stream)

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

## Connecting Functions
def start_stream_twitter(**kargs):
    """Return an iterator for the Twitter stream, if keywords 
    are supplied it switches to a filtered Twitter stream"""
    # May be able to set: 'track' (keywords), 'follow' (user IDS), and 
    # 'locations' (lat&long coordinates), 
    # https://dev.twitter.com/streaming/reference/post/statuses/filter
    # https://github.com/sixohsix/twitter/blob/master/twitter/stream_example.py
    auth = twitter.OAuth(
        consumer_key=TWITTER_CREDENTIALS['consumer_key'],
        consumer_secret=TWITTER_CREDENTIALS['consumer_secret'],
        token=TWITTER_CREDENTIALS['access_token'],
        token_secret=TWITTER_CREDENTIALS['access_token_secret']
    )
    twitter_public_stream = twitter.TwitterStream(auth=auth)
    if len(kargs) > 0:
        return twitter_public_stream.statuses.filter(**kargs)
    else: 
        return twitter_public_stream.statuses.sample()

def start_wordpress_stream(stream_url):
    """Return an iterator for any of the WordPress streams"""
    r = requests.get(stream_url, stream=True)
    return r.iter_lines()

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
        return tz.pipe(
            tz.get_in(['entities', 'hashtags'], given_item, default=[]),
            tz.map(lambda x: tz.get_in(['text'], x, default=None)),
            tz.filter(lambda x: x is not None),
            lambda x: ", ".join(x))
    def reformat_timestamp(given_ts):
        """Reformat into WordPress.com format"""
        # Twitter example: "Sat Oct 10 14:48:34 +0000 2015"
        # WordPress example: "2015-10-10T19:42:34Z"
        if given_ts is None:
            return ""
        try: 
            return tz.pipe(
                given_ts,
                lambda x: dt.datetime.strptime(x, "%a %b %d %H:%M:%S +0000 %Y"),
                lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ"))
        except: # If it can't reformat it, just use the previous version
            return str(given_ts)
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
        """Return a string of the tags associated with a post"""
        return tz.pipe(
            tz.get_in(['object', 'tags'], given_dict, default = []),
            tz.filter(lambda x: tz.get_in(['objectType'], x, default=None) == 'tag'),
            tz.map(lambda x: tz.get_in(['displayName'], x, default=None)),
            lambda x: ", ".join(x)
        )
    def get_categories(given_dict):
        """Return a string of the categories associated with a post"""
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
        'content_len': len_or_none(gv(['object', 'content'])), # presently includes the HTML markup
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
        'published': gv(['published']), # a date-time stamp
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
    try: 
        for num, row in enumerate(stream_iterator):
            stored_stream.append(row)
            if (num % save_size[CONFIG['mode']]) == 0 and num != 0: # occasionally save to disc
                stored_frame = pd.DataFrame(stored_stream)
                stored_frame.to_sql('stream', db_engine, if_exists='append', index=False)
                stored_stream = []
                log_update(stream_key, num)   # feedback for debugging
    except KeyboardInterrupt:
        # Save in-memory data when it receives a SIGINT
        stored_frame = pd.DataFrame(stored_stream)
        stored_frame.to_sql('stream', db_engine, if_exists='append', index=False)
        log_update(stream_key, num)   # feedback for debugging
            
    return True

def save_csv_gz(stream_key, stream_iterator):
    """Save the given stream to a compressed CSV file
    
    This is presently designed to write the rows in chunks
    rather than row by row. My thought was that if there was 
    overhead to each write, this chunking would reduce the number 
    of times the program needed to deal with that."""
    file_name = get_save_location(
        stream_key, 
        "_{}.csv.gz".format(time.strftime("%Y-%m-%d_%I-%M-%S"))) 
        # timestamp, to prevent overwriting when starting a new file
    save_size = {'debug':10, 'production': 1000}
    with gzip.open(file_name, "w") as f :
        stored_stream = []
        try: 
            for num, row in enumerate(stream_iterator):
                stored_stream.append(row)
                if num == 0:
                    writer = csv.DictWriter(f, fieldnames=row.keys())
                    writer.writeheader()
                if (num % save_size[CONFIG['mode']]) == 0 and num != 0:
                    writer.writerows(stored_stream) # save stored stream entries
                    stored_stream = []              # reset storage
                    log_update(stream_key, num)     # feedback for debugging
        except KeyboardInterrupt:
            # Save in-memory data and close the file when it receives a SIGINT
            writer.writerows(stored_stream) # save stored stream entries
            log_update(stream_key, num)     # feedback for debugging
    return True

def write_to_log(stream_key, what_to_write):
    """Save some output to a simple log"""
    with open('../data/log_{}.txt'.format(stream_key), 'a') as log:
        log.write(what_to_write)

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
    """A little function to peak in on streams"""
    print(given_item)
    return given_item

def log_update(stream_key, num):
    """Save a small update on how things are going"""
    update = "{} {} {}".format(
        stream_key.ljust(8), 
        dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        num)
    write_to_log(stream_key, update+"\n")
    print(update)

def permissive_json_load(given_item):
    """A version of json.loads that returns an empty dictionary if
    the given_item can't be decoded"""
    try:
        return json.loads(given_item)
    except:
        return {}

def print_twitter_stall_warning(given_item):
    """Print stall warnings, pass everything through"""
    warning = tz.get_in(['warning'], given_item, default = None)
    if warning is not None:
        write_to_log(STREAM_KEY, warning)
        print(warning) 
    return(given_item)

if __name__ == '__main__':
    main()
