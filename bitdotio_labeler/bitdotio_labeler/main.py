import pandas as pd
import numpy as np
import requests
from datetime import datetime
import time
import os
from bitdotio_pandas import BitDotIOPandas
from config import CONFIG, LOGO, INSTRUCTIONS
from queries import get_status_sql, get_leaderboard_sql, get_batch_sql, get_update_sql


def get_prompt(num_labels, df):
    '''Generates command prompt'''
    return f'''
    Please enter a command:                     {num_labels}/{df.shape[0]} examples in this batch labeled\n
                   prior example                                 
                       (w)                                  (x) neutral label - no sentiment
    negative label (a)     (d) positive label               (q)uit (CAUTION: have you uploaded your labels?)
                       (s)                                  (u)pload labels and reload examples
                   next example
    '''


def reset_display():
    '''Refresh console display'''
    os.system('clear')
    print(LOGO)    


def get_username():
    '''Get username for login'''
    while True:
        username = input("Please input your bit.io username: ")
        # For now only checks user existance, not a secure/high integrity approach
        if requests.get(f"https://bit.io/{username}").status_code == 200:
            break
        else:
            print("bit.io username not found.")
    return username


def get_api_key():
    '''Get API key for login'''
    while True: 
        api_key = input("Please input your bit.io api key: ")
        try:
            print("Attempting to connect to bit.io...")
            bpd = BitDotIOPandas(api_key = api_key, username=CONFIG['REPO_OWNER'], repo=CONFIG['REPO'])
            if CONFIG['DATASET_TABLE'] in bpd.list_tables():
                break
            else:
                print('Dataset table is not visible from this account. Please change accounts or ask the dataset owner to share with you.')
        except ValueError:
            print('Unable to connect with this api key. Are you sure what you provided is correct?')
    return bpd


def login():
    '''Log in with username and api key'''
    username = get_username()            
    reset_display()
    bpd = get_api_key()
    return username, bpd


def print_hline():
    print('********************************************************************')


def show_status(bpd):
    '''Shows the user the status of the labeling process'''
    print('Please wait while labeling status is retrieved...')
    df_status = bpd.read_sql(get_status_sql())
    df_leaderboard = bpd.read_sql(get_leaderboard_sql())
    reset_display()
    if df_leaderboard.shape[0] > 0:
        df_leaderboard = df_leaderboard.set_index(np.arange(1,df_leaderboard.shape[0] + 1))
    num_labeled, num_samples = df_status['num_labeled'].iloc[0], df_status['num_samples'].iloc[0]
    print('LABELING STATUS')
    print_hline()
    print(f"\nThis dataset has {num_labeled}/{num_samples} samples labeled. Let's keep it up!\n")
    print(f"Note: your first {CONFIG['NUM_OVERLAP']} samples will overlap with other users.\n")
    print('Here are the top label contributors:\n')
    print(df_leaderboard)
    print()
    print_hline()
    input('\nHit enter to continue.')


def show_instructions():
    '''Display labeling instructions to the users'''
    reset_display()
    print('INSTRUCTIONS')
    print_hline()
    print(INSTRUCTIONS)
    print_hline()
    print()
    input('Hit enter to continue.')


def get_samples(username, bpd):
    '''Get a new batch of samples to label'''
    reset_display()
    show_status(bpd)
    # Download sample of unlabeled records
    df = bpd.read_sql(get_batch_sql(username))
    df['contributor'] = username
    df[CONFIG['LABEL_COL']] = 'No label'
    df['timestamp'] = None
    return df


def add_label(idx, df, label, num_labels):
    '''Add a label to a sample'''
    if df[CONFIG['LABEL_COL']].iloc[idx] == 'No label':
         num_labels += 1
    df[CONFIG['LABEL_COL']].iloc[idx] = label
    df['timestamp'].iloc[idx] = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
    return min(idx + 1, df.shape[0] - 1), num_labels


def upload_labels(df, bpd, username):
    '''Upload a batch of labels'''
    reset_display()
    df_upload = df.loc[df[CONFIG['LABEL_COL']] != 'No label'][['id', 'contributor', 'manual_label', 'timestamp']]
    df_upload[CONFIG['LABEL_COL']] = df_upload[CONFIG['LABEL_COL']].map({
        'No label': np.nan,
        'Positive': 1,
        'Negative': 0,
        'Neutral': 0.5})
    print(f'Uploading {df_upload.shape[0]} labeled samples and drawing a new unlabeled batch...')
    # Upload labels to contribution table
    bpd.to_table(df_upload, CONFIG['LABEL_TABLE'])
    bpd.sql(get_update_sql())
    print('Upload successful!')
    time.sleep(2)
    # Get new unlabeled batch
    df = get_samples(username, bpd)
    return df


def init():
    '''Login, status, and batch initialization'''
    reset_display()
    print("Welcome to the bit.io data labeling tool!\n")
    username, bpd = login()
    show_instructions()
    return get_samples(username, bpd), username, bpd  


def main(df, username, bpd):
    idx = 0
    num_labels = 0
    while True:
        reset_display()
        row = df.iloc[idx,:]
        print(f'Example: {idx + 1}, Current label: {row.manual_label}')
        print_hline()
        print(row.body)
        print_hline()
        command = input(get_prompt(num_labels, df))
        if command.lower() == 'w':
            idx = max(0, idx - 1)
        elif command.lower() == 's':
            idx = min(df.shape[0] - 1, idx + 1)
        elif command.lower() == 'd':
            idx, num_labels = add_label(idx, df, 'Positive', num_labels)
        elif command.lower() == 'a':
            idx, num_labels = add_label(idx, df, 'Negative', num_labels)
        elif command.lower() == 'x':
            idx, num_labels = add_label(idx, df, 'Neutral', num_labels)
        elif command.lower() == 'u':
            df = upload_labels(df, bpd, username)
            idx, num_labels = 0, 0
        elif command.lower() == 'q':
            reset_display()
            command = input('Caution: make sure you have uploaded your labels before quitting. Are you sure? (y/n)\n')
            if command.lower() == 'y':
                break
        else: 
            print('Invalid input')


if __name__ == '__main__':
    df, username, bpd = init()
    main(df, username, bpd)


