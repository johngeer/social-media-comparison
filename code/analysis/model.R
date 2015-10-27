# This script loads the data and performs some initial wrangling. 
# It prepares the data for analysis, as a result it should be run 
# before the investigate_*.R scripts.
#
# The data loading is separated out into this file so that it 
# doesn't have to be repeated when running the analyses multiple 
# times. Loading the data into memory can be one of the more time 
# consuming steps, this helps avoid needing to repeat it too often. 
# :)

library(dplyr)

# Data Wrangling Functions
convert_minutes = function(given_df){
    # Convert the entire minute variable into POSIXct objects
    given_df['minute'] = do.call(
        c,
        lapply(given_df['minute'], convert_ts_to_date))
    return(given_df)
}
convert_ts_to_date = function(given_timestamp){
    # Convert the given timestamp into a POSIXct object
    given_timestamp %>% 
    as.character %>% 
    (function(x){as.POSIXct(x, format="%Y-%m-%dT%H:%M")})
}

# Load The Data
chunk_period = 30 # duration of a "chunk", in minutes

## Overall count data
overall_count_base = "../../data/demdebate/overall_count/"
overall_count_files = list(                          # specify filenames
    'comments' = sprintf("%s%s", overall_count_base, 'count_comments.csv'),
    'likes' = sprintf("%s%s", overall_count_base, 'count_likes.csv'),
    'posts' = sprintf("%s%s", overall_count_base, 'count_posts.csv'),
    'tweets' = sprintf("%s%s", overall_count_base, 'count_tweets.csv'))
overall_count_df = lapply(overall_count_files, read.csv) %>% # read in the files
    lapply(convert_minutes) %>%                      # set date format
    (function(x){                                    # combine into a single dataframe
        do.call(
            rbind, 
            x)})
blank_df = expand.grid(list(                         # build df with entries for all minutes
        'minute' = unique(overall_count_df$minute),
        'verb' = c("comment", "like", "post", "tweet"),
        'num_entries' = 0))
overall_count_df = overall_count_df %>% 
    right_join(blank_df, by=c("minute", "verb")) %>% # double check that every minute is included
    mutate(num_entries = ifelse(is.na(num_entries.x), num_entries.y, num_entries.x)) %>% 
    select(num_entries, minute, verb)
overall_count_chunked_df = overall_count_df %>%      # aggregated by "chunk" time interval
    group_by(verb, minute = as.POSIXct(
        minute %>% as.numeric - (minute %>% as.numeric %% (chunk_period*60)),
        origin = "1970-01-01 00:00.00 UTC")) %>% 
    summarize(num_entries = sum(num_entries), num_minutes = n()) %>% 
    filter(num_minutes == chunk_period)              # remove partial chunks

## Debate count data
debate_count_base = "../../data/demdebate/debate_count/"
debate_files = list(                                 # specify filenames
    'comments' = sprintf("%s%s", debate_count_base, 'count_comments.csv'),
    'likes' = sprintf("%s%s", debate_count_base, 'count_likes.csv'),
    'posts' = sprintf("%s%s", debate_count_base, 'count_posts.csv'),
    'tweets' = sprintf("%s%s", debate_count_base, 'count_tweets.csv'))
debate_count_df = lapply(debate_files, read.csv) %>% # read in the files
    lapply(convert_minutes) %>%                      # set date format
    (function(x){                                    # combine into a single dataframe
        do.call(
            rbind, 
            x)}) %>%
    right_join(blank_df, by=c("minute", "verb")) %>% # check that every minute is included
    mutate(num_entries = ifelse(is.na(num_entries.x), num_entries.y, num_entries.x)) %>% 
    select(num_entries, minute, verb)
debate_count_hourly_df = debate_count_df %>%         # aggregated by the hour
    group_by(verb, minute = as.POSIXct(
        minute %>% as.numeric - (minute %>% as.numeric %% 60**2), 
        origin = "1970-01-01 00:00.00 UTC")) %>% 
    summarize(num_entries = sum(num_entries), num_minutes = n()) %>% 
    filter(num_minutes == 60)                        # remove partial hours
debate_count_chunked_df = debate_count_df %>%        # aggregated by "chunk" time period
    group_by(verb, minute = as.POSIXct(
        minute %>% as.numeric - (minute %>% as.numeric %% (chunk_period*60)), 
        origin = "1970-01-01 00:00.00 UTC")) %>% 
    summarize(num_entries = sum(num_entries), num_minutes = n()) %>% 
    filter(num_minutes == chunk_period)              # remove partial chunks
