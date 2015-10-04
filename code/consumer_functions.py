import json                  # parsing JSON
import requests              # for working with HTTP
import cytoolz.curried as tz # functional programming library
import os                    # for using environment variables
import pdb                   # for testing
import twitter               # python twitter tools

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
    ## Python Twitter Tools
    auth = twitter.OAuth(
        consumer_key=TWITTER_CREDENTIALS['consumer_key'],
        consumer_secret=TWITTER_CREDENTIALS['consumer_secret'],
        token=TWITTER_CREDENTIALS['access_token'],
        token_secret=TWITTER_CREDENTIALS['access_token_secret']
    )
    twitter_userstream = twitter.TwitterStream(auth=auth)
    stream_iterator = twitter_userstream.statuses.sample()

    save_first(
        '../data/samples/example_twitter.json', 
        stream_iterator)

    return True

def main():
    pass

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

if __name__ == '__main__':
    main()
