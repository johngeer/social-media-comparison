source('model.R')
source('functions.R')

investigate_debate_anomalies(debate_count_chunked_df)

# save_as_png(
#     "debate_anomalies.png",
#     c(5, 8.09),
#     k(investigate_debate_anomalies(debate_count_chunked_df)))
