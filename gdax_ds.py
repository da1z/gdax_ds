import gdax
import datetime
import time
import enum
import pandas as pd

class Granularity(enum.Enum):
    MINUTE = 60
    FIVE_MINUTE = 300
    FIFTEEN_MINUTE = 900
    HOUR = 3600
    SIX_HOURS = 21600
    ONE_DAY = 86400

def get_df(product:str, start:datetime, end:datetime, 
    granularity:Granularity=Granularity.HOUR):      
    delta = datetime.timedelta(seconds=granularity.value * 300)
    _start = start
    data = []
    while _start != end:
        _end = min(_start + delta, end)
        data += _get_data(product, _start, _end, granularity.value)
        _start = _end
    df = pd.DataFrame(data=data, columns=['time', 'low', 'high', 'open', 'close', 'volume'])
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    df.sort_index(inplace=True)
    return df


def _get_data(product, start, end, granularity):
    client = gdax.PublicClient()
    for retry in range(0, 3):
        data = client.get_product_historic_rates(product, start, end, granularity)
        if isinstance(data, dict):
            time.sleep(1.5)
        else:
            break
    return data