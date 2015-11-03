source('model.R')
source('functions.R')

# investigate_debate_anomalies(debate_count_chunked_df)

d = 6.5
save_as_png(
    "debate_anomalies.png",
    c(d, d),
    k(investigate_debate_anomalies(debate_count_chunked_df)))
