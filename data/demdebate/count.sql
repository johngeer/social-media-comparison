-- To run this from the command line:
-- sqlite3 example.sqlite < count.sql

.headers on
.mode csv

.output count_comments.csv
select count(rowid) as num_entries, substr(published,0,17) as minute, 'comment' as verb
from comments
group by substr(published,0,17);

.output count_likes.csv
select count(rowid) as num_entries, substr(published,0,17) as minute, 'like' as verb
from likes
group by substr(published,0,17);

.output count_posts.csv
select count(rowid) as num_entries, substr(published,0,17) as minute, 'post' as verb
from posts
group by substr(published,0,17);

.output count_tweets.csv
select count(rowid) as num_entries, substr(created_at,0,17) as minute, 'tweet' as verb
from tweets
group by substr(created_at,0,17);
