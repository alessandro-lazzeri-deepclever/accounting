import pandas as pd
from pathlib import Path

def read_mastrini(path):
    mastrino_path = Path('../data/polaris/Settembre 2020/Mastrini con corrispettivi movimenti settembre.xls')

    return (pd.read_excel(mastrino_path, header=2, usecols='B:R'))

def clean_mastrini(df):
    # TODO: assign type to each column properly
    df['Data Doc.'] = pd.to_datetime(df['Data Doc.'],format='%d/%m/%Y')
    return df.dropna(axis=0, how='all')

def check_mastrini(df):
    print('Columns with all NaNs', df.isna().all(axis=0))
    print('Columns with some NaNs', df.isna().any(axis=0))

    print('Rows with all Nans', df.isna().all(axis=1).any())
    print('Rows with some Nans', df.isna().any(axis=1).any())
    print(df.dtypes)

if __name__ == '__main__':

    mastrino_path = Path('../data/polaris/Settembre 2020/Mastrini con corrispettivi movimenti settembre.xls')
    
    mastrini = clean_mastrini(read_mastrini(mastrino_path))
    
    check_mastrini(mastrini)
    print(mastrini.loc[mastrini['Generalità'] == 'Radar Leather Division'].iloc[0])
