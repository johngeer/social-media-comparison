# This script contains the R language analysis code. 
# 
# I wrote it all in one file, for easy re-use of helper functions.
# The investigate_*.R scripts run this code to perform particular 
# investigations.
library(dplyr)    # for data wrangling
library(ggplot2)  # for plotting
library(ggthemes) # for plot formatting
library(scales)   # for date formatting
library(forecast) # for fitting time series models

## Investigation functions
### These functions perform specific investigations and are usually
### called from an investigate_*.R script.
investigate_content_rate = function(rate_df){
    # Look into the rate at which events are published on the sample streams
    plot_df = trim_to_good_data(rate_df)

    guess_time = as.POSIXct("2015-10-13 21:00") %>% as.numeric # unknown event

    # Plot the data
    p = ggplot(plot_df, aes(x = minute, y = num_entries, colour = verb)) +
        geom_point(alpha = 0.2) +
        # geom_vline(xintercept = guess_time, lty=2) +
        # stat_smooth(method="loess", span=0.3, size=1, se=FALSE) +
        geom_line(stat="smooth",method = "loess", span=0.3, alpha = 1, colour="black") +
        facet_wrap(~verb, ncol=1, scales='free_y') +
        xlab("Time (UTC)") + 
        ylab("Sampled Publications Per Minute") +
        theme_tufte(base_size=15) +
        theme(legend.position="none") +
        scale_color_brewer(palette="Set2") +
        scale_x_datetime(labels = date_format("%b %-d\n%-H:%M"))
    print(p)
}

investigate_peaks = function(rate_df){
    # There appear to be some minutes with a much higher rate of publications,
    # especially for posts. This is an investigation into patterns in those
    # highs (peaks).
    plot_df = trim_to_good_data(rate_df) %>% 
        # Add variable indicating whether each entry is on the hour
        mutate(hourly = (minute %>% as.numeric %% 60**2) == 0) 
    
    # Plot the data
    p = ggplot(plot_df, aes(x = minute, y = num_entries, colour = hourly, alpha = hourly)) +
        geom_point() +
        scale_alpha_discrete(range = c(0.15, 0.8)) +
        stat_smooth(method="loess", span=0.3, alpha = 0.2) +
        facet_wrap(~verb, ncol=1, scales='free_y') +
        xlab("Time (UTC)") + 
        ylab("Sampled Publications Per Minute") +
        theme_tufte(base_size=15) +
        theme(legend.position="none") +
        scale_color_brewer(palette="Dark2") +
        scale_x_datetime(labels = date_format("%b %-d\n%-H:%M"))
    print(p)
}

investigate_debate_rate = function(rate_df){
    # Look into the rate of publications on the sample streams associated
    # with the democratic debate.
    plot_df = trim_to_good_data(rate_df) %>% 
        filter(verb %>% as.character != "like")
        # Remove likes, because I wasn't able to filter those to 
        # debate-related entries as well
    demdebate_times = c( # the times when the debate started and ended
        as.POSIXct("2015-10-14 00:30"), 
        as.POSIXct("2015-10-14 03:00")) %>% as.numeric

    # Plot the data
    p = ggplot(plot_df, aes(x = minute, y = num_entries, colour = verb)) +
        geom_point(alpha = 0.2) +
        geom_vline(xintercept = demdebate_times, lty=2) +
        geom_line(stat="smooth",method = "loess", span=0.2, alpha = 1, colour="black", se=TRUE) +
        # stat_smooth(method="loess", span=0.3, size=1, se=TRUE) +
        facet_wrap(~verb, ncol=1, scales='free_y') +
        xlab("Time (UTC)") + 
        ylab("Sampled Publications Per Minute") +
        theme_tufte(base_size=15) +
        theme(legend.position="none") +
        scale_color_brewer(palette="Set2") +
        scale_x_datetime(labels = date_format("%b %-d\n%-H:%M"))
    print(p)
}

investigate_debate_rate_hourly = function(hourly_rate_df){
    # Look into the rate of publications on the sample streams associated
    # with the democratic debate. This function assumes the data is hourly.
    plot_df = trim_to_good_data(hourly_rate_df) %>% 
        filter(verb %>% as.character != "like")
        # Remove likes, because I wasn't able to filter those to 
        # debate-related entries as well
    demdebate_times = c( # the times when the debate started and ended
        as.POSIXct("2015-10-14 00:30"), 
        as.POSIXct("2015-10-14 03:00")) %>% as.numeric

    # Plot the data
    p = ggplot(plot_df, aes(x = minute, y = num_entries, colour = verb)) +
        geom_point(alpha = 0.8) +
        geom_vline(xintercept = demdebate_times, lty=2) +
        # geom_line(stat="smooth",method = "loess", span=0.2, alpha = 1, colour="black", se=TRUE) +
        stat_smooth(method="loess", span=0.3, size=1, se=TRUE) +
        facet_wrap(~verb, ncol=1, scales='free_y') +
        xlab("Time (UTC)") + 
        ylab("Sampled Publications Per Hour") +
        theme_tufte(base_size=15) +
        theme(legend.position="none") +
        scale_color_brewer(palette="Set2") +
        scale_x_datetime(labels = date_format("%b %-d\n%-H:%M"))
    print(p)
}

investigate_debate_anomalies = function(rate_df){
    # Look into anomalies in the publication rates of the sample streams
    # that are associated with the democratic debate.
    add_forecast = function(given_ts, given_order, confidence_level){
        # Return a dataframe with the original timeseries and forecast variables
        offset = 10
        # Make predictions
        index_range = offset:length(given_ts)
        prediction_df = do.call(rbind, 
            lapply(
                index_range, 
                function(x){
                    forecast_by_index(given_ts, x, given_order, confidence_level)}))
        # Add offset
        offset_df = data.frame(
            rep(NA, offset-1),
            rep(NA, offset-1),
            rep(NA, offset-1),
            given_ts[1:(offset -1)], 
            rep(NA, offset-1))
        names(offset_df) = names(prediction_df)
        rbind(offset_df, prediction_df)
    }
    forecast_by_index = function(given_ts, given_index, given_order, confidence_level){
        # Return a forecast for the entry at index: given_index
        # Forecast is based on previous values with an Arima model.
        model = Arima(
            given_ts[1:(given_index - 1)],
            order=given_order,
            method="ML")
        prediction = forecast(model, h=1, level = confidence_level)
        data.frame(prediction, original=given_ts[given_index]) %>% 
            mutate(Unusual = !((original <= Hi.99) & (original >= Lo.99)))
    }

    # Analyze Comments
    comment_df = rate_df %>% 
        filter(verb %>% as.character == "comment") %>% 
        arrange(minute)
    comment_forecast = add_forecast( # add forecast variables
        ts(comment_df$num_entries),
        c(1, 0, 0), 
        99.0)
    comment_df = cbind(comment_df, comment_forecast)

    # Analyze Posts
    post_df = rate_df %>% 
        filter(verb %>% as.character == "post") %>% 
        arrange(minute)
    post_forecast = add_forecast( # add forecast variables
        ts(post_df$num_entries),
        c(1, 0, 0), 
        99.0)
    post_df = cbind(post_df, post_forecast)

    # Analyze Tweets
    tweet_df = rate_df %>% 
        filter(verb %>% as.character == "tweet") %>% 
        arrange(minute)
    tweet_forceast = add_forecast( # add forecast variables
        ts(tweet_df$num_entries),
        c(2, 1, 1), 
        99.0)
    tweet_df = cbind(tweet_df, tweet_forceast)

    # Plot
    plot_df = rbind( # Put the stream data frames together
        comment_df,
        post_df,
        tweet_df)
    demdebate_times = c( # The starting and ending times of the democratic debate
        as.POSIXct("2015-10-14 00:30"), 
        as.POSIXct("2015-10-14 03:00")) %>% as.numeric
    annotation_text = data.frame( # annotation
        minute = as.POSIXct("2015-10-14 12:00"),
        num_entries = 7500,
        verb = "tweet",
        Unusual = NA)
    p = ggplot(plot_df, aes(x=minute, y=num_entries, color=Unusual)) +
        facet_wrap(~verb, ncol=1, scales="free_y") +
        geom_point() +
        geom_vline(xintercept = demdebate_times, lty=2) +
        geom_line(stat="smooth",method = "loess", span=0.3, alpha = 1, colour="black", se=TRUE) +
        xlab("Time (UTC)") + 
        ylab("Sampled Publications Per Half Hour") +
        ggtitle("Rate of Debate-Related Publications") +
        theme_tufte(base_size=15) +
        scale_color_manual(values=c("#1b9e77", "#d95f02"), na.value = "grey50") +
        scale_x_datetime(labels = date_format("%b %-d\n%-H:%M")) +
        geom_text(data = annotation_text, label="First Democratic\nParty Debate", colour = "black", size=4)
    print(p)
}

investigate_publication_decay = function(rate_df, perc_diff_df=FALSE){
    ## Compare the rate that publications drop off in the different streams
    trim_to_after_debate = function(rate_df){
        ## Limit the rate dataframe to measurements after the debate
        rate_df %>%
            # 7 - 13
            filter(minute >= as.POSIXct("2015-10-14 07:00")) %>% 
            filter(minute < as.POSIXct("2015-10-15 13:00"))
            # I chose these dates because they include a complete day, and 
            # are in all of the series. The idea of using a complete day is 
            # an attempt to prevent the daily periodicity from biasing the 
            # models.
    }
    fit_exponential_decay_model = function(focused_df, verb_name){
        ## Fit an exponential decay model to the after debate data

        # Prepare data
        focused_df = focused_df %>% 
            filter(verb %>% as.character == verb_name) %>% # limit to a stream
            mutate(num_entries = ifelse(num_entries == 0, NA, num_entries)) # hack to prevent log problems

        # Fit model
        model = glm(log(num_entries) ~ hour, data = focused_df)

        # Check the fit
        # print(summary(model)) # check 
        # plot(model) 

        # Calculate the half-life of the model
        hl = log(2)/(-1 * model$coefficients[2]) 
        sprintf("The half life of this model is %s", hl) %>% print

        # Return data
        rbind(
            focused_df %>% 
                mutate(fit = rep(FALSE, n())), 
            focused_df %>% 
                ungroup %>% 
                mutate(
                    fit = rep(TRUE, n()),
                    # verb = sprintf("%s_fit", as.character(verb)),
                    num_entries = predict(model, newdata=focused_df) %>% exp))
    }
    compensate_for_periodicity = function(rate_df, perc_diff_df){
        # use the perc_diff column in the perc_diff_df to adjust the rate_df

        adjusting_df = perc_diff_df %>%
            select(verb, minute, perc_diff) # limit to the relvant columns

        rate_df %>% 
            left_join(adjusting_df, by=c("verb", "minute")) %>% 
            mutate(
                num_entries_raw = num_entries, # save raw stream
                # include the opposite percentage movement to what we see in the 
                # overall stream
                num_entries = ((-1 * perc_diff) + 1) * num_entries_raw) %>%
            identity
    }

    # Prepair data
    trimmed_df = rate_df %>% 
        trim_to_good_data %>%  # trim to the appropriate time frame
        trim_to_after_debate %>% 
        mutate(hour = (minute %>% as.numeric)/60**2) # add in hourly variable, for half-life
    if (is.data.frame(perc_diff_df)) {
        # adjust for periodicity, if we have that data
        trimmed_df = compensate_for_periodicity(trimmed_df, perc_diff_df)
    }

    # Fit models
    fit_df = rbind(
        fit_exponential_decay_model(trimmed_df, "tweet"),
        fit_exponential_decay_model(trimmed_df, "comment"),
        fit_exponential_decay_model(trimmed_df, "post"))

    # Plot
    p = ggplot(fit_df, aes(x = minute, y = num_entries, color = fit)) + 
        facet_wrap(~verb, ncol=1, scales="free_y") +
        geom_point() +
        xlab("Time (UTC)") + 
        ylab("Sampled Publications Per Half Hour") +
        ggtitle("Decay of Debate-Related Publications") +
        theme_tufte(base_size=15) +
        scale_color_brewer(palette="Set2") +
        scale_x_datetime(labels = date_format("%b %-d\n%-H:%M"))
    print(p)
}

## Helper functions
### These functions perform specific tasks, and are often reused
### across the investigations and model building.
trim_to_good_data = function(rate_df){
    # Trim the arrivals per minute data frame to minutes with complete data
    rate_df %>% 
        filter(
            minute >= as.POSIXct("2015-10-13 13:04"), # remove older posts
            (verb != "tweet" & minute <= as.POSIXct("2015-10-15 20:21")) |
            # remove final partial minute for comments, likes, and posts
            (verb == "tweet" & minute < as.POSIXct("2015-10-15 13:00")))
            # remove final partial minute for tweets
}
convert_to_edt = function(given_posixct){
    # Convert from UTC to EDT (-4:00)
    given_posixct - 4*(60**2)
}
k = function(given_value) { # constant function
    # Returns a function that always returns the given_value
    function(){return(given_value)}
}
save_as_svg = function(filename, dim, fun) {
    ## save the plotting output from a given function as an svg
    ## saves the output to filename with width dim[1] and height dim[2]
    ## example with ggplot: save_as_svg('plot.svg', c(5, 5), k(print(p)))
    svg(filename, width=dim[1], height=dim[2])
    fun()
    dev.off()
}
save_as_png = function(filename, dim, fun) {
    ## save the plotting output from a given function as a png
    ## saves the output to filename with width dim[1] and height dim[2]
    ## example with ggplot: save_as_png('plot.svg', c(5, 5), k(print(p)))
    # resolution (res) might be set a little high
    png(filename, width=dim[1], height=dim[2], units="in", res=100)
    fun()
    dev.off()
}
prep_ts = function(rate_df, verb_name, given_freq){
    # Prepare a time series for modeling
    model_df = rate_df %>% 
        trim_to_good_data %>% 
        filter(verb %>% as.character == verb_name) %>% 
        arrange(minute)
    ts(model_df$num_entries, freq=given_freq)
}
identity = function(x) { x }
get_percent_diff = function(rate_df){
    # Return the percentage above or below the mean each stream is
    add_percent_diff = function(filtered_df) {
        # return a new df with the percentage different variable added
        filtered_df %>% 
            mutate(perc_diff = (num_entries - mean(num_entries))/mean(num_entries))
    }
    rate_df %>% 
        group_by(verb) %>% 
        do(add_percent_diff(.))
}

## Model fitting
### These are functions that perform one-time investigations. Once 
### they are complete the information (like which model to use) is 
### incorporated in other functions.
decompose_stream = function(model_series){
    # Uses an additive decomposition to break a given time series 
    # into seasonal, trend, and random components.
    # These components are then plotted and returned
    d = decompose(model_series, type = "additive")
    plot(d)
    data.frame(
        num_entries_dup=d$x,
        periodicity=d$seasonal,
        trend=d$trend)
}
fit_comment_model = function(rate_df){
    # Determine which model would be appropriate for the comment data
    model_series = prep_ts(rate_df, "comment", 48)

    ## Initial investigation
    # There aren't notable trends in this series, so a differencing may not be necessary
    # decompose_stream(model_series)
    # acf(model_series)
    # pacf(model_series)
    # ACF and PACF indicate that the data has an AR 1 structure

    model = Arima(model_series, order=c(1, 0, 0))
    print(model)
    r = model$residuals
    # acf(r, 48*2)
    # pacf(r, 48*2)
    # This model seems to fit the data reasonably well

    seasonal_model = Arima(model_series, order=c(1, 0, 0), seasonal=c(1, 1, 0))
    print(seasonal_model)
    r = seasonal_model$residuals
    # acf(r, 48*2)
    # pacf(r, 48*2)
    # This seems to fit a bit better, but requires more history.

    # Because we don't have a full day of history before the democratic
    # debate, as seasonal model would not assist with identifying anomalies
    # during that event.
    # 
    # As a result, my plan is to use a non-seasonal ARIMA(1,0,0) for 
    # this stream.
}
fit_post_model = function(rate_df){
    # Determine which model would be appropriate for the post data
    model_series = prep_ts(rate_df, "post", 48)

    ## Initial Investigation
    # The series seems to be stationary as is, so short-term differencing has been skipped
    # decompose(model_series)
    # plot(model_series)
    # acf(model_series)
    # pacf(model_series)

    model = Arima(model_series, order=c(1, 0, 0))
    print(model)
    r = model$residuals
    plot(r)
    # acf(r, 48*2)
    # pacf(r, 48*2)
    # An AR 1 model seems to fit best. While there are some indications of 
    # some moving average patterns in the data, moving average terms don't 
    # seem to improve the model or the residuals.

    # Fitting a seasonal model to this data seems to result in over fitting
    # seasonal_model = Arima(model_series, order=c(1, 0, 0), seasonal=c(1, 1, 0))
    # print(seasonal_model)
    # r =  seasonal_model$residuals
    # plot(r)
    # acf(r, 48*2)
    # pacf(r, 48*2)
}
fit_tweet_model = function(rate_df){
    # Determine which model would be appropriate for the tweet data
    model_series = prep_ts(rate_df, "tweet", 48)

    ## Initial Investigation
    # decompose_stream(model_series)
    # plot(model_series)
    # plot(diff(model_series, 1))
    # acf(model_series)
    # pacf(model_series)

    model = Arima(model_series, order=c(2, 1, 1))
    print(model)
    r = model$residuals
    par(mfrow=c(2,1))
        acf(r)
        pacf(r)
    par(mfrow=c(1,1))

    # Because we don't have a full day of history before the democratic
    # debate, a seasonal model would not assist with identifying anomalies
    # during that event.
}
