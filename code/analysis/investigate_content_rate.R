source('model.R')
source('functions.R')

save_as_png(
    "plot_content_rate.png",
    c(5, 8.09),
    k(investigate_content_rate(overall_count_df)))
