library(dplyr)

# Data Wrangling Functions
convert_ts_to_date = function(given_timestamp){
    # Convert the given timestamp to an official R timestamp 
    given_timestamp %>% 
    as.character %>% 
    (function(x){as.POSIXct(x, format="%Y-%m-%dT%H:%M")})
}

convert_minutes = function(given_df){
    # Convert all the minute strings to POSIXct objects
    given_df['minute'] = do.call(
        c,
        lapply(given_df['minute'], convert_ts_to_date))
    return(given_df)
}

# Load The Data
## Overall count data
count_base = "../../data/demdebate/overall_count/"
demdebate_files = list(                              # specify filenames
    'comments' = sprintf("%s%s", count_base, 'count_comments.csv'),
    'likes' = sprintf("%s%s", count_base, 'count_likes.csv'),
    'posts' = sprintf("%s%s", count_base, 'count_posts.csv'),
    'tweets' = sprintf("%s%s", count_base, 'count_tweets.csv'))
demdebate_df = lapply(demdebate_files, read.csv) %>% # read in the files
    lapply(convert_minutes) %>%                      # set date format
    (function(x){                                    # combine into a single dataframe
        do.call(
            rbind, 
            x)})
blank_df = expand.grid(list(                         # build df with entries for all minutes
        'minute' = unique(demdebate_df$minute),
        'verb' = c("comment", "like", "post", "tweet"),
        'num_entries' = 0))
demdebate_df = demdebate_df %>% 
    right_join(blank_df, by=c("minute", "verb")) %>% # double check that every minute is included
    mutate(num_entries = ifelse(is.na(num_entries.x), num_entries.y, num_entries.x)) %>% 
    select(num_entries, minute, verb)

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
