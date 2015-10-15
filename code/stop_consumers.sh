# Stop the consumers that run_all.sh starts

kill -INT `cat consumers.pid`

# Delete the consumers file, so it won't try to stop the same pids again
rm consumers.pid
