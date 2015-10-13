## These are some tests for the consumer functions
## They can be run with pytest with the command `py.test test_consumers.py`

import consumer_functions as cf
import json

## Tests of Parsing Functions
def test_parse_tweet():
    with open("test_data/example_twitter.json", 'r') as f:
        result = cf.parse_tweet(json.loads(f.read()))
        assert result == {
            'user_lang': u'ru', 
            'count_urls': 1, 
            'text': u'RT @HumanFeeds: 11 Super Romantic Morning Texts Everyone Wants To Wake Up To http://t.co/k7NmOkwweW http://t.co/g8h825vL6J', 
            'hashtags': '', 
            'is_quote_status': False, 
            'user_favourites': 524, 
            'user_scree_name': u'pogorelov_m459', 
            'user_id': 2611369848, 
            'created_at': '2015-10-06T22:27:31Z', 
            'time_zone': None, 
            'timestamp_ms': u'1444170451657', 
            'is_reply': False, 
            'count_media': 1, 
            'is_retweet': True}

    test1 = cf.parse_tweet({'created_at': 'what the what', 'entities':{'hashtags': [{'text': 'first'}]}})
    assert test1['hashtags'] == 'first'
    assert test1['created_at'] == 'what the what'

    test2 = cf.parse_tweet({'entities':{'hashtags': ['first', 'second', 'third']}})
    assert test2['hashtags'] == ''

    test3 = cf.parse_tweet({'entities':{'hashtags': [{'text': 'first'}, {'text': 'second'}]}})
    assert test3['hashtags'] == 'first, second'

    test4 = cf.parse_tweet({'created_at': 10})
    assert test4['created_at'] == "10"

    test5 = cf.parse_tweet({'created_at': None})
    assert test5['created_at'] == ""

    test6 = cf.parse_tweet({})
    assert test6['created_at'] == ""

## Tests of Helper Functions
def test_get_value_if_present_nested():
    assert cf.get_value_if_present_nested({'first': 1}, ['first']) == 1
    assert cf.get_value_if_present_nested({'first': {'second': 1}}, ['first', 'second']) == 1
    assert cf.get_value_if_present_nested({'first': {'second': 1}}, ['first', 'third']) is None

def test_permissive_json_load():
    assert cf.permissive_json_load("""{"test": 10}""") == {'test': 10}
    assert cf.permissive_json_load("""Not really json""") == {}
