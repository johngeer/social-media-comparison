# Run all the stream consumers as background processes

# For Development
find . -name '*.pyc' -delete # remove compiled files

nohup python consumer_functions.py comments &
echo $! >> consumers.pid # save the pid's, to make it easier to stop them
nohup python consumer_functions.py posts &
echo $! >> consumers.pid
nohup python consumer_functions.py likes &
echo $! >> consumers.pid
nohup python consumer_functions.py tweets &
echo $! >> consumers.pid
# nohup python consumer_functions.py filtered_tweets &
# echo $! >> consumers.pid
