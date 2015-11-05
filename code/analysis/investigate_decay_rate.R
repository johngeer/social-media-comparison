source('model.R')
source('functions.R')

d = 6.5
# Without Periodicity Compensation
save_as_png(
    "decay_rate.png",
    c(d, d),
    k(investigate_publication_decay(debate_count_chunked_df)))

# With Periodicity Compensation
t = get_percent_diff(overall_count_chunked_df) # variation based on overall series
save_as_png(
    "decay_rate_compensated.png",
    c(d, d),
    k(investigate_publication_decay(debate_count_chunked_df, t)))
