import os.path
import time
from datetime import datetime

from adapter import initialize_headers, get_ggal_options_names, get_prices

if __name__ == '__main__':
    if not os.path.isdir('data'):
        os.mkdir('data')
    os.environ['TOKEN'] = '505E3F28-E40F-4AE5-80C5-D823786CA516'
    initialize_headers(token=os.environ['TOKEN'])
    options_names = []
    while len(options_names) == 0:
        options_names = get_ggal_options_names()
        time.sleep(60)
    print(options_names)
    while True:
        print(f'Retrieving data at {str(datetime.now())}')
        get_prices('GGAL', plazo=1)
        for option_name in options_names:
            get_prices(option_name, plazo=1)

        time.sleep(30)