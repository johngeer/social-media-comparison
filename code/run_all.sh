# Run all the stream consumers as background processes

# For Development
find . -name '*.pyc' -delete # remove compiled files

python consumer_wordpress_comments.py &
python consumer_wordpress_posts.py &
python consumer_wordpress_likes.py &
python consumer_tweets.py &
