-- To run this from the command line:
-- sqlite3 demdebate.sqlite < demdebate_filter.sql

PRAGMA case_sensitive_like=OFF; -- make like case insensitive

create table if not exists comments_debate as
select * 
from comments
where 
    -- debate focused
    content like "%democrat%" or
    content like "%debate%" or 
    content like "%cnn%" or 
    -- candidate focused
    content like "%hillary%" or
    content like "%clinton%" or
    content like "%bern%" or
    content like "%sanders%" or
    content like "%o'malley%" or
    content like "%omalley%" or
    content like "%webb%" or
    content like "%chafee%"
    ;

create table if not exists likes_debate as
select * 
from likes
where 
    -- debate focused
    displayName like "%democrat%" or
    displayName like "%debate%" or 
    displayName like "%cnn%" or 
    -- candidate focused
    displayName like "%hillary%" or
    displayName like "%clinton%" or
    displayName like "%bern%" or
    displayName like "%sanders%" or
    displayName like "%o'malley%" or
    displayName like "%omalley%" or
    displayName like "%webb%" or
    displayName like "%chafee%"
    ;

create table if not exists posts_debate as 
select *
from posts
where
    -- debate focused
    summary like "%democrat%" or
    summary like "%debate%" or 
    summary like "%cnn%" or 
    -- candidate focused
    summary like "%hillary%" or
    summary like "%clinton%" or
    summary like "%bern%" or
    summary like "%sanders%" or
    summary like "%o'malley%" or
    summary like "%omalley%" or
    summary like "%webb%" or
    summary like "%chafee%" or

    --#-- in the content
    -- debate focused
    content like "%democrat%" or
    content like "%debate%" or 
    content like "%cnn%" or 
    -- candidate focused
    content like "%hillary%" or
    content like "%clinton%" or
    content like "%bern%" or
    content like "%sanders%" or
    content like "%o'malley%" or
    content like "%omalley%" or
    content like "%webb%" or
    content like "%chafee%" or

    --#-- tags
    -- debate focused
    tags like "%democrat%" or
    tags like "%debate%" or 
    tags like "%cnn%" or 
    -- candidate focused
    tags like "%hillary%" or
    tags like "%clinton%" or
    tags like "%bern%" or
    tags like "%sanders%" or
    tags like "%o'malley%" or
    tags like "%omalley%" or
    tags like "%webb%" or
    tags like "%chafee%"
    ;

create table if not exists tweets_debate as
select *
from tweets
where 
    -- debate focused
    text like "%democrat%" or
    text like "%debate%" or 
    text like "%cnn%" or 
    -- candidate focused
    text like "%hillary%" or
    text like "%clinton%" or
    text like "%bern%" or
    text like "%sanders%" or
    text like "%o'malley%" or
    text like "%omalley%" or
    text like "%webb%" or
    text like "%chafee%" or

    -- debate focused
    hashtags like "%democrat%" or
    hashtags like "%debate%" or 
    hashtags like "%cnn%" or 
    -- candidate focused
    hashtags like "%hillary%" or
    hashtags like "%clinton%" or
    hashtags like "%bern%" or
    hashtags like "%sanders%" or
    hashtags like "%o'malley%" or
    hashtags like "%omalley%" or
    hashtags like "%webb%" or
    hashtags like "%chafee%"
    ;

-- Save some counts to CSV
.headers on
.mode csv

.output count_comments.csv
select count(rowid) as num_entries, substr(published,0,17) as minute, 'comment' as verb
from comments_debate
group by substr(published,0,17);

.output count_likes.csv
select count(rowid) as num_entries, substr(published,0,17) as minute, 'like' as verb
from likes_debate
group by substr(published,0,17);

.output count_posts.csv
select count(rowid) as num_entries, substr(published,0,17) as minute, 'post' as verb
from posts_debate
group by substr(published,0,17);

.output count_tweets.csv
select count(rowid) as num_entries, substr(created_at,0,17) as minute, 'tweet' as verb
from tweets_debate
group by substr(created_at,0,17);
