## bitdotio_labeler

The bitdotio_labeler is a simple tool that we built to enable team
data labeling of r/wallstreetbet comments.

This tool can easily be adapted for similar text labeling problems.

## Contents
- bitdotio_pandas.py - a work-in-progress general helper class that abstracts the interface between bitdotio and pandas
- config.py - most of the configuration parameters you need to change to adapt the tool to your own text labeling problem
- main.py - the main script for the simple CLI app that displays comments and uploads labels
- queries.py - functions for generating SQL queries used by the main script

## Need help?
- This is a quick prototype and admittedly not great code (yet). If you need help, please open a Github issue or simply email doss@bit.io. We are happy to help you adapt this tool for your own text labeling problem. 

## The database
- The [database used  for this application](https://bit.io/bitdotio/stonks/#) is publicly readable but is only writable for our specified collaboraters. 

## A demo
- We really like collaborating using Deepnote and set up [a publicly visible project](https://deepnote.com/project/wsblabelblog-W3UmuI59S0SFk6Zs92yDeQ/%2FSETUP_README.ipynb) that we used for this team data labeling effort. Using Deepnote, we can jump into a shared Python environment and open several terminals at once to label data in parallel without any local environment setup.  
You can copy the Deepnote project and open a terminal to demo the labeling tool in read-only mode, or point the tool at your own bit.io repo where you have write access to demo the full capability. 
