from pathlib import Path
import numpy as np
import pandas as pd
pd.set_option('display.max_columns', None)
from src.reader_fattura import make_df


def join_on_importo(bank_movement, invoice, field_name = 'Addebiti'):
    join = pd.merge(bank_movement, invoice, left_on=field_name,
                    right_on='FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_ImportoTotaleDocumento')
    return join


def date_filter(df):
    return df.loc[df['Data'] > df['FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Data']]


def select(df):

    return df[['Data','Valuta','Descrizione','Addebiti','Descrizione Estesa',
               'FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Numero',
               'FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Data']].drop_duplicates()


if __name__ == '__main__':

    path_to_file = Path(r'C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\ESTRATTI CONTO\ListaMovimenti 01.11.2020  28.02.2021.xls')
    invoice_rootdir = r'C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\16.03.2020-31.12.2020\Fatture in Cloud\acquisti elettronici'

    save_path_match_bank_invoice_date_filter = r'..\result\match_bank_invoice_data_filter.xlsx'
    save_path_match_bank_invoice = r'..\result\match_bank_invoice.xlsx'

    #skip = list(range(0,18))
    #skip.extend(list(range(19,20)))
    #df = pd.read_excel(path2file,header=0,usecols="A:F",skiprows=skip, nrows=665)
    df = pd.read_excel(path_to_file, header=18, usecols="A:F", skiprows=[19])
    last_row_index = df.loc[df['Descrizione'] == 'Saldo contabile finale in Euro'].index[0]

    bank_movement = df[df.index < last_row_index]


    invoice = make_df(invoice_rootdir)

    print('bank movement', bank_movement.shape)
    print('bank movement addebiti', bank_movement.loc[bank_movement['Addebiti'] > 0].shape)
    print('bank movement accrediti', bank_movement.loc[bank_movement['Accrediti'] > 0].shape)
    print('invoice', invoice.shape)

    join = pd.merge(bank_movement, invoice, left_on='Addebiti',
                    right_on='FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_ImportoTotaleDocumento')

    df = join_on_importo(bank_movement, invoice)
    #join['FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Data'] = pd.to_datetime(join['FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Data'])

    #join = join.loc[join['Data']>join['FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Data']]
    df = date_filter(df)
    df.to_excel(save_path_match_bank_invoice_date_filter)
    df = select(df)
    print(df.shape)

    df = df[['Data', 'Valuta', 'Descrizione', 'Addebiti', 'Descrizione Estesa',
    'FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Numero',
    'FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Data']]
    print(df.shape)
    #print(df.dtypes)
    gb = df.groupby(['Data', 'Valuta', 'Descrizione', 'Descrizione Estesa'], as_index=False).\
        agg({'FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Data':np.min})

    res = pd.merge(df,gb, on=['Data', 'Valuta', 'Descrizione', 'Descrizione Estesa',
                              'FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Data'])
    print(res.shape)
    res.to_excel(save_path_match_bank_invoice)