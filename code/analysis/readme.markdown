# Analysis Code

This folder contains code to analyze the data saved from the streams.

### Structure

#### R Scripts (Rate and Timing Investigation)

The R scripts presently focus on the rate that events appears in the streams. The code is separated into the different files as follows: 

* `model.R`
    * Loading the data and does some initially tidying of it
    * Written to be run prior to any of the other files
* `functions.R`
    * Functions to perform specific types of analyses, as well as create visualizations.
* Code to run specific analyses
    * These are the `investigate_*.R` files such as
        * investigate_content_rate.R
        * investigate_debate_rate.R
        * ...
    * These are written to run and import `model.R` and `functions.R` and then perform specific investigations.

#### Python Scripts (Distinct Words Investigation)

This folder also contains the code for finding "distinct words" in a given stream. These are words that are notable for being used in one stream at a much higher rate than they are used overall.

This code is in the `distinctive_words.py` script and saves it's output as an html file to the `distinct_words_display` folder. 

The format that it uses for generating the html output is in the `templates` folder.

### Running These Scripts

The R scripts expect the following packages to be installed: 

* dplyr
* ggplot2
* ggthemes
* scales
* forecast

The packages that the python scripts expect are listed in the `requirements.txt` document in the parent folder.* However, the natural language processing packages also require one to download some pre-fit models. These are used in the scripts for tasks like part of speech tagging and lemmatization. 

The script will prompt you to download these models (and tell you which ones to use) when it needs them. However, I think you can just download all of them with the following command: 

`python -m textblob.download_corpora`

\* The (faster) perceptron tagger that this script uses by default will need to be downloaded from github. This can be done with pip by using the following command: 

`pip install git+https://github.com/sloria/textblob-aptagger.git@dev`
