import os

import pandas as pd

years = [x for x in range(2016, 2021)]
months = [x for x in range(1, 13)]


def parse_to_years(path):
    chunksize = 10 ** 5
    for chunk in pd.read_csv(path, chunksize=chunksize):
        chunk['departure'] = pd.to_datetime(chunk['departure'])
        chunk['year'] = pd.DatetimeIndex(chunk['departure']).year
        chunk['month'] = pd.DatetimeIndex(chunk['departure']).year
        for year_ in years:
            for month_ in months:
                df_to_csv = chunk.where((chunk['year'] == year_) & (chunk['month'] == month_))
                df_to_csv = df_to_csv[df_to_csv.notnull()]
                del df_to_csv['year']
                del df_to_csv['month']
                csv_path = 'result/' + str(month_) + '-' + str(year_) + '.csv'
                if os.path.exists(csv_path):
                    df_to_csv.to_csv(csv_path, mode='a', index=False, header=False)
                else:
                    df_to_csv.to_csv(csv_path, mode='w', index=False, header=True)


if __name__ == '__main__':
    parse_to_years('data/database.csv')
