import sqlalchemy as sqlal # for connecting to databases
import pandas as pd        # for data wrangling
import unicodecsv as csv   # for saving to CSV in UTF-8 by default
import toolz.curried as tz # functional programming library
import gzip                # for compression of CSV output
import time                # for some simple benchmarking
import sys                 # for interacting with the system

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

## Main Functions
@timed
def main():
    file_base = "../../data/demdebate/"
    database = "demdebate.sqlite"
    file_information = {
        'comments': {
            'csv_gz': "comments_stream_2015-10-13_09-03-40.csv.gz",
            'table_name': 'comments'},
        'likes': {
            'csv_gz': "likes_stream_2015-10-13_09-03-40.csv.gz",
            'table_name': 'likes'},
        'posts': {
            'csv_gz': "posts_stream_2015-10-13_09-03-40.csv.gz",
            'table_name': 'posts'},
        'tweets': {
            'csv_gz': "tweets_stream_2015-10-13_09-03-40.csv.gz",
            'table_name': 'tweets'}}
    keys = ['comments', 'likes', 'posts', 'tweets']
    for key in keys:
        try: 
            transfer_csvgz_sqlite(
                "{}{}".format(file_base, file_information[key]['csv_gz']),
                file_information[key]['table_name'],
                "{}{}".format(file_base, database))
        except IOError:
            # These files are presently truncated, so when it gets to the end
            # of the file it discovers that the last bit is broken.
            pass

def transfer_csvgz_sqlite(file_name, table_name, save_location):
    """Transfer data from a gzipped CSV file to a SQLite database for
    easier wrangling."""
    saveing_function = save_to_sqlite(table_name, save_location)
    load_csvgz_in_chunks(file_name, saveing_function)

## Helper Functions
def load_csvgz_in_chunks(file_name, saveing_function):
    """Load data from a gzipped CSV and pass it to a save function
    in chunks."""
    csv.field_size_limit(sys.maxsize)
    chunk_size = 3000
    with gzip.open(file_name, "r") as f :
        stored_stream = []
        reader = csv.DictReader(f)
        for num, row in enumerate(reader):
            stored_stream.append(row)
            if (num % chunk_size) == 0 and num != 0:
                saveing_function(stored_stream)
                stored_stream = []
    return True

@tz.curry
def save_to_sqlite(table, save_location, list_of_dictionaries):
    """Save the given list of dictionaries to the specified SQLite database"""
    db_engine = sqlal.create_engine('sqlite:///{}'.format(save_location))
    given_df = pd.DataFrame(list_of_dictionaries)
    given_df = given_df[sorted(given_df.columns.tolist())]
    given_df.to_sql(table, db_engine, if_exists='append', index=False)
    return True

if __name__ == '__main__':
    main()
