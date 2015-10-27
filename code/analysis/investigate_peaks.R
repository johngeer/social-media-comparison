source('model.R')
source('functions.R')

save_as_png(
    "plot_peaks.png",
    c(5, 8.09),
    k(investigate_peaks(overall_count_df)))
