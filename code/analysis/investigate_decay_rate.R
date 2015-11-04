source('model.R')
source('functions.R')

d = 6.5
save_as_png(
    "decay_rate.png",
    c(d, d),
    k(investigate_publication_decay(debate_count_chunked_df)))
