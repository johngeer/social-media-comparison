# Social Media Comparison

This is a project to collect and compare data from Twitter and WordPress.com. The hope is that this can help us better understand the differences between these two networks in terms of the type of content posted and the timing of its publication. 

This code can connect to, parse, and save events from Twitter's public stream and the public WordPress.com Posts, Comments, and Likes streams. 

### How to Run The Code

##### How to Start The Stream Consumers

These stream consumers are designed to be called from separate jobs via the command line. They are called this way to reduce the likelihood that the consumers of the different streams will conflict with each other. To start an individual stream consumer, one calls the python script `code/consumer_functions.py` followed by the name of the stream it should save (options include `tweets`, `filtered_tweets`, or WordPress `likes`, `posts`, or `comments`). So to save the likes stream, one would use the command: 

    python consumer_functions.py likes

The script `code/run_all.sh` is provided to make it more convenient to collect all of the streams at once. To start all five of the consumers as separate background jobs one can call:

    bash -i run_all.sh

This script will save the pid's of the jobs to a `code/consumers.pid` file. This makes it easier to find and stop them later. To make this a little easier, a shell script is also included that can stop all the jobs started by `run_all.sh`. To use this script to stop all the jobs, call:

    bash -i stop_consumers.sh

##### Configuration

The script's behavior can be configured by editing the `code/config.yaml` file. This allows one to adjust the format in which the events are saved, how often it saves and prints updates, the URLs for the WordPress.com streams, or the filtering options for the Twitter stream.

##### Authentication

The WordPress.com streams that this code uses are public and can be received without authentication. The Twitter stream is also public, but requires authentication credentials by creating a [Twitter App](https://apps.twitter.com/). This code requires those credentials to be set as the following environment variables:

* TWITTER_ACCESS_TOKEN
* TWITTER_ACCESS_SECRET
* TWITTER_CONSUMER_KEY
* TWITTER_CONSUMER_SECRET

##### Requirements

This script relies on several python libraries. A list of the versions that I used in development are in the `code/requirements.txt` file. This file is formated so that the packages can be installed in a virtual environment with the command: 

    pip install -r requirements.txt

### How to Read The Data

The data is saved to the `data/` folder as either a SQLite databases or a gzipped CSV files. One can specify which format the script should use in `code/config.yaml`.
