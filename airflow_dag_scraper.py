# import pandas as pd
# from bs4 import BeautifulSoup
# import requests
# import json
# import re

# from session_key import session_key

# """# DAG overview"""

# cookies = {
#     'session': session_key,
# }

# headers = {
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
#     'Accept-Language': 'en-US,en;q=0.9',
#     'Connection': 'keep-alive',
#     'Referer': 'http://192.168.88.135:8080/home?status=active',
#     'Upgrade-Insecure-Requests': '1',
#     'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
# }

# params = {
#     'status': 'active',
# }
# response = requests.get('http://192.168.88.135:8080/home', params=params, cookies=cookies, headers=headers, verify=False)

# soup = BeautifulSoup(response.content, 'html.parser')

# a_functions = []
# for tag in soup.find_all("a", {"class": "label label-default schedule"}):
#     func = {}
#     func['command'] = tag.get("data-dag-id")
#     func['schedule'] = tag.text.strip()
#     a_functions.append(func)
    
# a_functions = pd.DataFrame(a_functions)
# a_functions.to_csv('airflow_dags.csv', index=False)


import pandas as pd
from bs4 import BeautifulSoup
import requests

from session_key import session_key

def extract_dag_data(session_key, url, params, headers, verify=False):
    """
    Extract the DAG data from the specified URL.

    Parameters:
        session_key (str): The session key for authentication.
        url (str): The URL of the DAG data.
        params (dict): The query parameters for the URL.
        headers (dict): The headers for the HTTP request.
        verify (bool, optional): Flag indicating whether to verify the SSL certificate.
        
    Returns:
        pandas.DataFrame: The extracted DAG data.
    """
    cookies = {
        'session': session_key
    }

    response = requests.get(url, params=params, cookies=cookies, headers=headers, verify=verify)
    soup = BeautifulSoup(response.content, 'html.parser')

    a_functions = []
    for tag in soup.find_all("a", {"class": "label label-default schedule"}):
        func = {}
        func['command'] = tag.get("data-dag-id")
        func['schedule'] = tag.text.strip()
        a_functions.append(func)

    return pd.DataFrame(a_functions)

def main():
    """
    Main entry point of the script.
    """
    url = 'http://192.168.88.135:8080/home'
    params = {
        'status': 'active'
    }
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Referer': 'http://192.168.88.135:8080/home?status=active',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    }
    verify = False

    dag_data = extract_dag_data(session_key, url, params, headers, verify)
    dag_data.to_csv('airflow_dags.csv', index=False)

if __name__ == '__main__':
    main()