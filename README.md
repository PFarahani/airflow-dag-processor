# Airflow Dataset Preprocessing

This repository contains tools for extracting and preparing Airflow DAGs for further analysis.

## Scripts

### airflow_dag_scraper.py

This module provides the ability to extract information from Airflow dags and store it in a csv file for later analysis.

#### Requirements

The script requires the following libraries to be installed:

- `pandas`
- `BeautifulSoup`

**Note:** Access to the Airflow UI requires a session key, which can be manually extracted from your browser and saved in a `session_key.py` file. This file will be imported in the main script for authentication purposes.
  
#### Installation

1. Clone this repository
2. Navigate to the cloned directory in your terminal
3. Run the following command in your terminal
   ```
   pip install pandas beautifulsoup4
   ```

#### Usage

To extract information from the Airflow dags and store it in a csv file, run the following command in your terminal:

```
python airflow_dag_scraper.py
```

The output csv file, named `airflow_dags.csv`, will be stored in the same directory as the script.



### airflow_dataset_preprocessing.py

This script performs the preprocessing step for the [cron-task-scheduler](https://github.com/PFarahani/cron-task-scheduler.git) project. It analyzes the log data of Airflow dags and the information about the dags to prepare a summarized runtime report. The information includes calculation of average and maximum runtimes, failed runs, and its statistics. The final output is saved in a new file named `dags.csv`.

#### Requirements

The script requires the following libraries to be installed:

- `pandas`
- `numpy`

The script requires two mandatory datasets:

- The output of `airflow_dag_scraper` script
- The log file of Airflow dags

#### Usage
To use the script, run the following command in the terminal:

```
python airflow_dataset_preprocessing.py
```

The script will prompt the user to input the path to the log file and airflow_dag_scraper output file. After doing so, the script will perform the processing and saves the result in `dags.csv` file.