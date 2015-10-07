import json                  # parsing JSON
import requests              # for working with HTTP
import cytoolz.curried as tz # functional programming library
import os                    # for using environment variables
import pdb                   # for testing
import twitter               # python twitter tools
import pandas as pd          # for data wrangling
import numpy as np

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
pd.set_option('io.hdf.default_format','table') # set queryable table as default HDF5 format

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
    ## Python Twitter Tools
    auth = twitter.OAuth(
        consumer_key=TWITTER_CREDENTIALS['consumer_key'],
        consumer_secret=TWITTER_CREDENTIALS['consumer_secret'],
        token=TWITTER_CREDENTIALS['access_token'],
        token_secret=TWITTER_CREDENTIALS['access_token_secret']
    )
    twitter_userstream = twitter.TwitterStream(auth=auth)
    stream_iterator = tz.pipe(
        twitter_userstream.statuses.sample(),
        tz.filter(is_tweet),
        tz.map(parse_twitter_item)
    )

    stored_stream = []
    for item in enumerate(stream_iterator):
        if item[0] > 1000:
            break
        else:
            stored_stream.append(parse_twitter_item(item[1]))
    stored_frame = pd.DataFrame(stored_stream, dtype=str)

    types = stored_frame.apply(lambda x: pd.lib.infer_dtype(x.values))
    print types
    # for col in types[types=='unicode'].index:
    #   stored_frame[col] = stored_frame[col].astype(str)
    #
    # stored_frame.columns = [str(c) for c in stored_frame.columns]

    pdb.set_trace()
    store_compressed = pd.HDFStore('store_compressed.h5', complevel=9, complib='blosc', encoding="utf-8")
    store_compressed.append('stored_frame', stored_frame, data_columns=True)


    # save_first(
    #     '../data/samples/example_twitter.json', 
    #     stream_iterator)

    return True

def main():
    pass

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
## Parsing Functions
def parse_twitter_item(given_dict):
    """Reorganize the resulting dictionary"""
    def len_or_none(given_item):
        """return length, if it has one. Otherwise None"""
        try: 
            return len(given_item)
        except:
            return None
    # print reduce(value_if_present, ['user', 'id'], given_dict)
    gv = get_value_if_present_nested(given_dict)
    return {
        'timestamp_ms': gv(['timestamp_ms']),
        'created_at': gv(['created_at']),
        'text': gv(['text']),
        'hashtags': ', '.join(tz.pipe( #TODO: this doesn't seem to be working
            ['entities','hashtags'], 
            gv, 
            lambda x: tz.pluck('text', x) if (x is not None and isinstance(x, dict)) else [])),
        'is_quote_status': gv(['is_quote_status']),
        'user_id': gv(['user','id']),
        'user_name': gv(['user','name']),
        'user_lang': gv(['user','lang']),
        'user_favourites': gv(['user','favourites_count']),
        'count_urls': len_or_none(gv(['entities','urls'])),
        'count_media': len_or_none(gv(['entities','media'])),
        'truncated': gv(['truncated']),
        'is_reply': gv(['in_reply_to_user_id']) is not None,
        'is_retweet': gv(['retweeted_status']) is not None}

## Helper Functions
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
def get_value_if_present(given_dict, key_string):
    """Return the value associated with key_string, or None"""
    if isinstance(given_dict, dict) and key_string in given_dict.keys():
        return given_dict[key_string]
    else:
        return None
@tz.curry
def get_value_if_present_nested(given_dict, key_list):
    """Like value_if_present but can handle nesting"""
    return reduce(get_value_if_present, key_list, given_dict)

if __name__ == '__main__':
    connect_to_twitter_stream()
    main()
