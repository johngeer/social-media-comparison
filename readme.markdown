# Social Media Comparison

This is a project to collect and compare data from Twitter and WordPress.com. The hope is that this can help us better understand the differences between these two networks in the type of content and the timing of its publication. 

### How to Run The Code

Presently, only the stream consumers are written. These connect to Twitter's public stream and the public WordPress.com Posts, Comments, and Likes streams. The code for these consumers is in the code/consumer_functions.py script. 

##### How to Start The Stream Consumers

These stream consumers are designed to be called from separate jobs via the command line. They are called this way to reduce the likelihood that the consumers of the different streams will conflict with each other. To call all of the stream consumers at once, one can just run the run_all.sh shell script.

##### Authentication

The WordPress.com streams that this code uses are public and can be received without authentication. The twitter stream is also public, but requires authentication credentials by creating a [Twitter App](https://apps.twitter.com/). This code requires those credentials to be set as the following environment variables:

* TWITTER_ACCESS_TOKEN
* TWITTER_ACCESS_SECRET
* TWITTER_CONSUMER_KEY
* TWITTER_CONSUMER_SECRET
