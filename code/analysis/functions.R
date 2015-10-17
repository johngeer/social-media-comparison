library(dplyr)
library(ggplot2)
library(ggthemes)

## Helper functions
trim_to_good_data = function(rate_df){
    # Trim the arrivals per minute data frame to minutes with complete data
    rate_df %>% 
        filter(
            minute >= as.POSIXct("2015-10-13 13:04"), # remove older posts
            (verb != "tweet" & minute <= as.POSIXct("2015-10-15 20:21")) |
            # remove final partial minute for comments, likes, and posts
            (verb == "tweet" & minute <= as.POSIXct("2015-10-15 13:10")))
            # remove final partial minute for tweets
}

## Investigation functions
investigate_content_rate = function(rate_df){
    # Look into the rate at which events are published on the sample streams
    plot_df = trim_to_good_data(rate_df)

    demdebate_times = c(
        as.POSIXct("2015-10-14 00:30"), 
        as.POSIXct("2015-10-14 03:00")) %>% as.numeric
    guess_time = as.POSIXct("2015-10-13 21:00") %>% as.numeric

    p = ggplot(plot_df, aes(x = minute, y = num_entries, colour = verb)) +
        geom_point(alpha = 0.2) +
        # geom_vline(xintercept = demdebate_times, lty=2) +
        # geom_vline(xintercept = guess_time, lty=2) +
        stat_smooth(method="loess", span=0.3, size=1, se=FALSE) +
        facet_wrap(~verb, ncol=1, scales='free_y') +
        xlab("Time (UTC)") + 
        ylab("Sampled Publications Per Minute") +
        theme_tufte(base_size=15) +
        theme(legend.position="none") +
        scale_color_brewer(palette="Set2")
    print(p)
}

investigate_peaks = function(rate_df){
    # There appear to be some minutes with a much higher rate of publications,
    # especially for posts. This is an investigation into patterns in those
    # highs.
    plot_df = trim_to_good_data(rate_df)
    
    p = ggplot(plot_df, aes(x = minute, y = num_entries, colour = hourly, alpha = hourly)) +
        geom_point() +
        scale_alpha_discrete(range = c(0.15, 0.8)) +
        stat_smooth(method="loess", span=0.3, alpha = 0.2) +
        facet_wrap(~verb, ncol=1, scales='free_y') +
        xlab("Time (UTC)") + 
        ylab("Sampled Publications Per Minute") +
        theme_tufte(base_size=15) +
        theme(legend.position="none") +
        scale_color_brewer(palette="Dark2")
    print(p)
}
