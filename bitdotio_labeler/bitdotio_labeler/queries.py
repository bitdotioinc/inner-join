from config import CONFIG


def get_fully_qualified(table):
    return f'''"{CONFIG['REPO_OWNER']}/{CONFIG['REPO']}"."{table}"'''


def get_status_sql():
    sql = f'''
            SELECT
              SUM(CASE manual_label IS NOT NULL WHEN True THEN 1 ELSE 0 END) AS "num_labeled",
              COUNT(1) AS "num_samples"
            FROM {get_fully_qualified(CONFIG['DATASET_TABLE'])}
    '''
    return sql


def get_leaderboard_sql():
    sql = f'''
            SELECT
              contributor,
              COUNT(1) AS num_labels
            FROM {get_fully_qualified(CONFIG['LABEL_TABLE'])}
            GROUP BY contributor
            ORDER BY num_labels DESC
            LIMIT 5
    '''
    return sql


def get_batch_sql(username):
    '''This prioritizes the overlap subset, then the unlabeled dataset'''
    sql = f'''SELECT DS.id, DS.body
              FROM {get_fully_qualified(CONFIG['DATASET_TABLE'])} AS DS
              LEFT JOIN (SELECT DISTINCT id, contributor
                         FROM {get_fully_qualified(CONFIG['LABEL_TABLE'])}
                         WHERE contributor = '{username}') AS UL
              ON DS.id = UL.id
              WHERE {CONFIG['LABEL_COL']} IS NULL OR (subset = 1 AND contributor IS NULL)
              ORDER BY subset DESC, RANDOM()
              LIMIT {CONFIG['BATCH_SIZE']}
    '''
    return sql


def get_update_sql():
    '''Generates sql to update the comments sample'''
    sql = f'''
        UPDATE {get_fully_qualified(CONFIG['DATASET_TABLE'])} SET (num_manual_labels, manual_label) = (0, NULL);
        UPDATE {get_fully_qualified(CONFIG['DATASET_TABLE'])} AS CS SET (num_manual_labels, manual_label) =
        (SELECT
           num_manual_labels,
           manual_label
         FROM (SELECT
                 id,
                 COUNT("manual_label") AS num_manual_labels,
                 AVG("manual_label") AS manual_label
                 FROM {get_fully_qualified(CONFIG['LABEL_TABLE'])}
         GROUP BY id) AS CSML
         WHERE CS."id" = CSML."id");
    '''
    return sql
