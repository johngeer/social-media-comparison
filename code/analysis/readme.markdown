# Analysis Code

This folder contains code to analyze the data saved from the streams.

### Structure

The R scripts presently focus on the rate that events appears in the streams. The code is separated in to the different files as follows: 

* `model.R`
    * Loading the data and does some initially tidying of it
    * Written to be run prior to any of the other files
* `functions.R`
    * Functions to perform specific types of analyses, as well as create visualizations.
* Code to run specific analyses
    * These are the other `*.R` files such as
        * investigate_content_rate.R
        * investigate_debate_rate.R
        * ...
    * These are written to run and import `model.R` and `functions.R` and then perform specific investigations.
