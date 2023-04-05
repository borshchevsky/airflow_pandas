import logging

import requests


def get_json(url):
    for retry in range(10):
        try:
            return requests.get(url, timeout=5).json()
        except requests.exceptions.ConnectionError:
            logging.error(f'Error fetching {url}. Retrying ({retry})...')
