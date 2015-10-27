source('model.R')
source('functions.R')

investigate_debate_rate_hourly(debate_count_hourly_df)

# save_as_png(
#     "debate_rate_hourly.png",
#     c(5, 8.09),
#     k(investigate_debate_rate_hourly(debate_count_hourly_df)))
