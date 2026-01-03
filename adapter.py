import os
from datetime import datetime

import pandas as pd
import requests
import json

headers = {}


def initialize_headers(token: str) -> None:
    global headers
    headers = {
        'Accept': 'application/json',
        'Authorization': token,
        'Content-Type': 'application/json',
        'Lang': 'es',
        'Referer': 'https://clientes.balanz.com/app/detalleinstrumento?ticker=GGAL&idPlazo=2&eventoptfrom=tabla-cotizaciones&action_from=tabla-cotizaciones',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15'
    }
    url = 'https://clientes.balanz.com/api/v1/banners'

    try_login = requests.get(url, headers=headers)
    if try_login.status_code != 200:
        raise Exception("Error when logging in with current token")


def filter_last_seq_number(df: pd.DataFrame, csv_path: str):
    saved_df = pd.read_csv(csv_path)
    last_sequence_number = saved_df['nrosecuencia'].max().item()
    # Getting last sequence number
    # with open(csv_path) as file:
    #     for last_line in file:
    #         pass

    # Filter
    # last_sequence_number = int(last_line.split(',')[0])
    return df[df['nrosecuencia'] > last_sequence_number]


def serialize_df(
        df: pd.DataFrame,
        prefix: str,
        filter_by_seq_number: bool = True,
        mode: str = 'a'
) -> None:
    path = "data/"
    path += "-".join([prefix, str(datetime.now().date())])
    path += '.csv'

    create_file = False if os.path.isfile(path) and mode != 'w' else True
    if filter_by_seq_number and not create_file:
        df = filter_last_seq_number(df, path)

    if len(df) > 0:
        df.to_csv(path, mode=mode, header=create_file)
    else:
        if 'GGAL' in prefix:
            print(f'Warning {prefix} df is empty.')


def get_prices(ticker: str, plazo: int):
    global headers
    url = f'https://clientes.balanz.com/api/v1/cotizacionintradiario?ticker={ticker}&plazo={plazo}&detalle=1&agrupado=0'

    try:
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            raise Exception(f'Error when retrieving {ticker} history')

        content_dict = json.loads(r.content)['intradiario']
        df = pd.DataFrame.from_dict(content_dict)

        if len(df) > 0:
            df.sort_values(by=['nrosecuencia'], inplace=True)

        serialize_df(df, f'OPERATIONS-{ticker}')

    except Exception as e:
        print(f"{str(datetime.now())} - {str(e)}")
        return pd.Series([])


def get_ggal_options_names() -> pd.Series:
    global headers
    url = 'https://clientes.balanz.com/api/v1/cotizaciones/opciones?token=0&tokenindice=0&avoidAuthRedirect=true'

    try:
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            raise Exception('Error when retrieving GGAL history')

        content_dict = json.loads(r.content)
        df = pd.DataFrame.from_dict(content_dict['cotizaciones'])

        # Filter by GGAL
        df = df[df['id'].str.startswith('GFG')]

        serialize_df(df, 'METADATA-OPTIONS-GGAL', filter_by_seq_number=False, mode='w')

        return df['id']
    except Exception as e:
        print(f"{str(datetime.now())} - {str(e)}")
        return pd.Series([])


