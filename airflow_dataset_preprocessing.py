import pandas as pd
import numpy as np
from datetime import datetime

def preprocess_data(dag_log, dag_info):
    """
    Preprocessing the data for dag_log and dag_info datasets

    Parameters:
    dag_log (pandas.DataFrame): The log file of Airflow dags
    dag_info (pandas.DataFrame): The output of `airflow-dag-scraper` script

    Returns:
    pd.DataFrame: The preprocessed data for dags
    """
    # Calculating runtime
    dag_log['avg_runtime'] = pd.to_datetime(dag_log['end_date']) - pd.to_datetime(dag_log['start_date'])

    # Calculating avg_runtime
    avg_runtime = dag_log[(dag_log['run_type'] == 'scheduled') & (dag_log['state'] == 'success')].groupby('dag_id')['avg_runtime'].mean()

    # Calculating max_runtime
    max_runtime = dag_log[dag_log['state'] == 'success'].groupby('dag_id')['avg_runtime'].max()

    # Create a new dataframe with dag_id and max_runtime
    new_df = pd.DataFrame({'dag_id': max_runtime.index, 'max_runtime': max_runtime.values})

    # Add avg_runtime to the new dataframe
    new_df = new_df.merge(avg_runtime.to_frame(), on='dag_id', how='left')

    # Create the failed_runs column
    new_df["failed_runs"] = new_df.apply(lambda row: {
        "num_fails": (dag_log[(dag_log["dag_id"] == row["dag_id"]) & (dag_log['state'] == 'failed')])['run_type'].ne('manual').sum(),
        "avg_run_time": dag_log[(dag_log["dag_id"] == row["dag_id"]) & (dag_log['state'] == 'failed')]['avg_runtime'].mean()
            if (dag_log[dag_log["dag_id"] == row["dag_id"]])['run_type'].ne('manual').sum() != 0 else None,
        "max_run_time": dag_log[(dag_log["dag_id"] == row["dag_id"]) & (dag_log['state'] == 'failed')]['avg_runtime'].max()
            if (dag_log[dag_log["dag_id"] == row["dag_id"]])['run_type'].ne('manual').sum() != 0 else None
    }, axis=1)

    # # Unsuccessful runs
    # dag_log[dag_log['state'] != 'success'].groupby('dag_id')['state'].count()
    # dag_log.groupby('dag_id').agg({'runtime': ['mean', 'max'], 'state': lambda x: (x != 'success').sum()})

    # Merging the two datasets
    dags = pd.merge(new_df, dag_info, left_on='dag_id', right_on='command', how='right').drop('command', axis=1)
    
    return dags

def main():
    dag_log_file = input("Enter the path to the dag log file: ")
    dag_info_file = input("Enter the path to the dag info file: ")

    dag_log = pd.read_csv(dag_log_file)
    dag_info = pd.read_csv(dag_info_file, index_col=0)
    dags = preprocess_data(dag_log, dag_info)
    dags.to_csv('dags_{}.csv'.format(datetime.now().date()), index=False)

if __name__ == '__main__':
    main()