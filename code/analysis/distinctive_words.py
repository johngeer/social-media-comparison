# This looks through the content from the different streams to find 
# 'distinctive words'. These are words that are the most likely to come
# from a given stream. For example "RT" tends to be a distinctive word for
# the twitter stream because it is frequently used it tweets (to mean 
# retweet) yet is rarely used in the other streams. 
#
# These distinctive words are intended to provide a sense of how the content 
# differs between these streams. 
# 
import sqlalchemy as sqlal                     # for connecting to databases
import pandas as pd                            # for data wrangling
import toolz.curried as tz                     # functional programming library
from bs4 import BeautifulSoup                  # for handling html
import nltk                                    # for natural language parsing
from textblob import TextBlob                  # for part of speech tagging
from textblob_aptagger import PerceptronTagger # for part of speech tagging
import langdetect                              # For estimating the language of some text
import re                                      # regular expressions
import datetime as dt                          # for handling stream timestamps
import time                                    # for simple benchmarking
import jinja2                                  # for generating html
import pdb                                     # for debugging

## Decorators
def timed(func):
    """A decorator to print the execution time of a given function.
    Helpful for simple benchmarking"""
    def new_func(*args, **kargs):
        start_time = time.time()
        result = func(*args, **kargs)
        print("The {} function took {}".format(func.__name__, time.time() - start_time))
        return result
    return new_func

## Main Functions
@timed
def main():
    """Find and display the distinct words for the different streams"""
    db_engine = sqlal.create_engine('sqlite:///../../data/demdebate/demdebate.sqlite')

    ## Penn Tagset
    verb_tags = ['VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']
    adjective_tags = ['JJ', 'JJR', 'JJS']
    noun_tags = ['NN', 'NNS', 'NNP', 'NNPS', 'PDT', 'POS', 'PRP', 'PRP$', 'WP', 'WP$']

    configuration = {
        ## stream_names identify the tables to pull data from in the database
        'stream_names': ['comments_debate', 'posts_debate', 'tweets_debate'], 
        ## overall time period to calculate values for
        'overall_date_range': ["2015-10-13T23:30:00Z", "2015-10-14T05:00:00Z"], # debate focused
        # 'overall_date_range': ["2015-10-14T00:30:00Z", "2015-10-14T03:00:00Z"], # debate only
        # 'overall_date_range': ["2015-10-14T01:00:00Z", "2015-10-14T02:00:00Z"], # testing
        'time_step': 30, # in minutes
        'max_num_words': 30, # number of words to return for a given time step
        # Which parts of speech to include (also accepts the string 'all')
        'allowed_parts_of_speech': verb_tags + adjective_tags + noun_tags, 
    }
    distinct_words = compare_streams_across_time(db_engine, configuration)
    save_as_html(distinct_words, "distinct_words_display/test.html")

def compare_streams_across_time(db_engine, configuration):
    """Return distinct words for each considered stream at each time step in 
    the given date range."""
    def date_range_iterator(overall_date_range, time_step):
        """Returns an iterator of the time ranges being considered.
        time_step is assumed to be in minutes"""
        def get_time(overall_start, time_step, step):
            """Return the timestamp that is step time_step's beyond overall_start"""
            return (overall_start + (time_step*(step-1))).strftime("%Y-%m-%dT%H:%M:%SZ")
        overall_start = dt.datetime.strptime(overall_date_range[0], "%Y-%m-%dT%H:%M:%SZ")
        overall_end = dt.datetime.strptime(overall_date_range[1], "%Y-%m-%dT%H:%M:%SZ")
        time_step = dt.timedelta(minutes=time_step)
        return tz.pipe(
            # Number of steps to take
            (overall_end - overall_start).total_seconds() / time_step.total_seconds(), 
            int,
            # Build range
            lambda x: range(1,x+2), 
            # Convert to timestamps
            tz.map(lambda x: [
                get_time(overall_start, time_step, x-1), 
                get_time(overall_start, time_step, x)]))
    result = []
    for date_range in date_range_iterator(configuration['overall_date_range'], configuration['time_step']):
        result.append(
            tz.pipe( # Stream comparison for a particular time period
                compare_streams(
                    db_engine,
                    date_range,
                    configuration['stream_names'],
                    configuration['allowed_parts_of_speech'],
                    configuration['max_num_words']),
                lambda x: tz.merge(x, {'date_range': date_range}))) # add in date_range entry
    return result

def compare_streams(db_engine, date_range, stream_names, allowed_parts_of_speech, max_num_words):
    """Compare tokens from each stream in the stream_names list"""

    ## Create token count dictionaries for each stream name
    count_dicts_dict = {}
    for stream_name in stream_names:
        count_dicts_dict[stream_name] = tz.pipe(
            get_content(
                db_engine, 
                stream_name,
                date_range),
            parse_content_into_count(max_num_words, allowed_parts_of_speech))

    ## Create cross-stream count dictionary
    all_streams_count_dict = reduce(
        lambda x,y: tz.merge_with(sum, x, y),
        count_dicts_dict.values())

    ## Calculate posterior probabilities of the tokens
    posterior_probs = {}
    for stream_name in stream_names:
        posterior_probs[stream_name] = tz.pipe(
            get_posterior_probs_freq(
                500, # limited to the 500 most frequent words in this stream, at this time
                all_streams_count_dict, 
                count_dicts_dict[stream_name]),
            tz.map(lambda x: tz.merge({"stream":stream_name}, x)),
            tz.take(max_num_words),
            list,
        )
    return posterior_probs

def save_as_html(distinct_words, file_name):
    """Generate and save an html display of the distinct words"""
    ## Wrangle data for presentation
    # Convert tokens into a single string
    def get_token_string(given_values):
        """Return a token string, if the given values are a list of dictionaries"""
        # check if it is a list of token-related information
        if (isinstance(given_values, list) and 
            len(given_values) > 0 and
            isinstance(given_values[0], dict)):
            return tz.pipe(
                given_values,
                tz.map(lambda x: x['token']),
                tz.map(wrap_in_highlight_link), # wrap in link to highlight words
                tz.reduce(lambda x,y: u"{}, {}".format(x, y)))
        # return empty string for empty lists
        elif isinstance(given_values, list) and len(given_values) == 0:
            return ''
        # check if it is a date range in need of formating
        elif isinstance(given_values, list) and len(given_values) == 2:
            return format_date_range(given_values)
        else:
            return given_values
    def format_date_range(given_date_range):
        """Return a pretty version of the given date_range"""
        date_range = map(
            lambda x: dt.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"),
            given_date_range)
        return "{} to {} UTC".format(
            date_range[0].strftime("%Y-%m-%d %H:%M"),
            date_range[1].strftime("%H:%M"))
    def wrap_in_highlight_link(given_string):
        """return the given string wrapped in the html code to highlight 
        other occurances of that same word"""
        return u"""<a href="javascript:void($('.distinct_words').removeHighlight().highlight('{string}'));">{string}</a>""".format(string=given_string)
    formated_distinct_words = tz.pipe(
        distinct_words,
        tz.map(
            tz.valmap(get_token_string)),
        list)

    ## Send to Template For Display
    template_dir = 'templates'
    loader = jinja2.FileSystemLoader(template_dir)
    environment = jinja2.Environment(loader=loader)
    template = environment.get_template('distinct_words.html')
    with open(file_name, 'w') as f:
        tz.pipe(
            template.render(distinct_words = formated_distinct_words),
            lambda x: x.encode('utf8'),
            lambda x: f.write(x))

## Helper Functions
def get_content(db_engine, stream_name, date_range):
    """Return content saved from the given stream in the given data range"""
    if stream_name in ['tweets', 'tweets_debate']:
        date_column = 'created_at'
        content_column = 'text'
    else:
        date_column = 'published'
        content_column = 'content'
    query = """
        select * 
        from {stream_name} 
        where {date_column} >= '{lower_date}' and {date_column} < '{upper_date}'"""\
            .format(stream_name = stream_name,
                    date_column = date_column,
                    lower_date = date_range[0], 
                    upper_date = date_range[1])
    returned_table = pd.read_sql_query(query, db_engine)
    return returned_table[content_column].tolist()

@tz.curry
@timed
def parse_content_into_count(max_num_words, allowed_parts_of_speech, list_of_content):
    """Return a dictionary of tokens (as keys) and counts (as values)"""
    def is_english(s):
        """Predicate that estimates whether a given string is in English"""
        try: 
            return langdetect.detect(s) == 'en'
        except:
            print("Couldn't detect the language of: {}".format(s))
            return True
    @tz.curry
    def tokenize_and_filter_perc_func(allowed_parts_of_speech, given_text):
        """Return the tokens in the given text that are the allowed parts
        of speech

        This version uses the faster PerceptronTagger"""
        return tz.pipe(
            given_text,
            lambda x: TextBlob(x, pos_tagger=PerceptronTagger()),
            lambda x: x.tags,
            tz.filter(lambda x: x[1] in allowed_parts_of_speech), 
                # limit to allowed parts of speech
            tz.map(lambda x: x[0]), # return only the token
            list, 
        )
    @tz.curry
    def tokenize_and_filter_nltk_func(allowed_parts_of_speech, given_text):
        """Return the tokens in the given text that are the allowed parts
        of speech

        This version uses the recommended tagger from NLTK, it is relatively
        slow."""
        return tz.pipe(
            given_text,
            nltk.word_tokenize,
            lambda x: nltk.pos_tag(x),
            tz.filter(lambda x: x[1] in allowed_parts_of_speech), 
                # limit to allowed parts of speech
            tz.map(lambda x: x[0]), # return only the token
            list, 
            print_and_pass)
    if allowed_parts_of_speech == "all":
        # Don't even tag parts of speech, just use everything
        tokenize_func = lambda x: nltk.word_tokenize(x)
    else: 
        tokenize_func = tokenize_and_filter_perc_func(allowed_parts_of_speech)
    wordnet_lemmatizer = nltk.stem.WordNetLemmatizer()
    lemma_fun = lambda x: wordnet_lemmatizer.lemmatize(x)
    exclusion_list = ['//platform.twitter.com/widgets.js', 'align=', 'aligncenter', 'id=', 'width=', '/caption', 'pdf.pdf', u'//t.c\xe2rt', 'http']
        # Yeah, this is a bit of a hack
    return tz.pipe(
        list_of_content, # given content
        tz.map(lambda x: BeautifulSoup(x, 'html.parser').get_text()), # remove html in string
        tz.filter(is_english), # limit to English entries
        tz.map(lambda x: re.sub(r'http.*?(?=\s)', "", x)), # remove urls
        chunk_string(500), # this is done to speedup the part of speech tagging
        tz.mapcat(tokenize_func), # tokenize, and maybe filter by part of speech
        tz.filter(lambda x: x not in exclusion_list), # filter out specific tokens
        tz.filter(lambda x: re.sub(r'\W', "", x) != ''), # filter out punctuation-only strings
        tz.map(lambda s: s.lower()), # convert to lower case
        tz.map(lemma_fun), # convert tokens to a more standard lemma
        tz.countby(tz.identity)) # count occurrences

@tz.memoize
def calculate_prior(num_tokens_all_streams, num_tokens_this_stream):
    """Calculate the prior probability that this token is from this stream"""
    return float(num_tokens_this_stream)/float(num_tokens_all_streams) 

def calculate_likelihood(num_tokens_in_stream, num_this_token_in_stream):
    """Calculate the likelihood of this word, given the stream"""
    return float(num_this_token_in_stream)/float(num_tokens_in_stream)

def calculate_evidence(num_tokens_across_streams, num_this_token_across_streams):
    """Calculate the probability of this token, across all streams"""
    return float(num_this_token_across_streams)/float(num_tokens_across_streams)

@tz.curry
def calculate_posterior(all_streams_count_dict, this_stream_count_dict, token):
    """Calculate the posterior probability of this token coming from 
    this stream."""
    num_tokens_all_streams = count_total_tokens(all_streams_count_dict)
    num_tokens_this_stream = count_total_tokens(this_stream_count_dict)
    prior = calculate_prior(
        num_tokens_all_streams,
        num_tokens_this_stream)
    likelihood = calculate_likelihood(
        num_tokens_this_stream,
        this_stream_count_dict[token])
    evidence = calculate_evidence(
        num_tokens_all_streams,
        all_streams_count_dict[token])
    return (prior * likelihood) / evidence

def count_total_tokens(count_dict):
    """Count the total number of tokens in the given count dictionary"""
    return sum(count_dict.values())

def get_top_tokens(n, count_dict):
    """Return the top n most frequent tokens in the count_dict
    If n > len(count_dict), it will just return them all"""
    return tz.pipe(
        count_dict,
        lambda x: x.items(),
        lambda x: sorted(x, key=lambda y: -y[1]),
        lambda x: tz.take(n, x),
        list)

def get_posterior_probs_freq(num_words, all_streams_count_dict, this_stream_count_dict):
    """Return the posterior probabilities for the num_words most frequent tokens
    in this_stream_count_dict"""
    occurance_minimum = 5 # the number of times a token must occur to be included
    return tz.pipe(
        get_top_tokens(num_words, this_stream_count_dict),
        tz.filter(lambda x: all_streams_count_dict[x[0]] >= occurance_minimum),
        tz.map(lambda x: {
            'token': x[0], 
            'occurrences': x[1],
            'posterior': calculate_posterior(
                all_streams_count_dict,
                this_stream_count_dict,
                x[0])}),
        lambda x: sorted(x, key=lambda y: -y['posterior']))


def print_and_pass(x):
    """Simple function for peaking into data pipelines"""
    print(x)
    return x

@tz.curry
def chunk_string(size, given_iterator):
    """Iterator function that takes an iterator of strings, and produces a stream
    of *size* of them concatenated together"""
    storage = ''
    for num, x in enumerate(given_iterator):
        storage = storage + x
        if num % size == 0:
            yield storage
            storage = ''
    yield storage

if __name__ == '__main__':
    main()
