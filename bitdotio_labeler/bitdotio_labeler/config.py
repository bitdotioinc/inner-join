CONFIG = {
    'REPO_OWNER': 'bitdotio',
    'REPO': 'stonks',
    'DATASET_TABLE': 'comments_sample',
    'LABEL_TABLE': 'comments_sample_manual_labels',
    'LABEL_COL': 'manual_label',
    'BATCH_SIZE': 100,
    'NUM_OVERLAP': 300
}


LOGO = '''
        @@@@@@@@@                                                               
    %@@@        @@@@@         @@            @@@   @@@         @@@               
   @@   @@@#,     @@@@,       @@                  @@@                           
  @@ @@   *@@     @@@@@       @@@@@@@@@#    @@@  @@@@@@       @@@    @@@@@@@@   
 @@@@@.@%       &@.@@@@@      @@@     @@@#  @@@   @@@         @@@  @@@/     @@@ 
  @@@@@     ##    @@ @@       @@       @@@  @@@   @@@         @@@  @@@      %@@ 
   @@@@     @@@@@  .@@        @@@@    @@@   @@@   @@@         @@@  %@@@    @@@@ 
     @@@@       (@@@.           (@@@@@@     @@@    @@@@  @@@  @@@     @@@@@@\n    
'''


INSTRUCTIONS = '''
Positive labels for comments indicating anticipation of a stock and/or the market
gaining, propensity to buy/hold, making money, etc.\n
Negative labels for comments indicating anticipation of a stock and/or the market
dropping, propensity to sell/short, losing money, etc.\n
Neutral labels for comments that don't indicate a positive/negative sentiment about
a stock or the market, per the definitions provided above.
'''
