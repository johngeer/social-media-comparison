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
count_base = "../../data/demdebate/overall_count/"
demdebate_files = list(
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
