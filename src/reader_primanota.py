"""
Convert from rtf to xlsx by copying and paste the tables
"""

import xlrd
from pathlib import Path

import pandas as pd
pd.set_option('display.max_columns', None)

ROW_IDS = [0,1,2,3,4,9,11,15,17,19,20]
DOC_ID = 2
DOC_ID_FORMAT = "object"
HEADER_IDS = [8,10,12,13,14,16,18]
EMPTY_CELL = ''
ROW_LABEL = [
    "Key",
    "Data Reg.",
    "N. Doc.",
    "Data Doc.",
    "Causale",
    "Conto",
    "Rag. Sociale / Descrizione",
    "Importo",
    "D/A",
    "G",
    "A",
    "ND ori."
]

HEAD_LABEL = [
    "Imponibile",
    "%al.",
    "rg.",
    "%ven",
    "Imposta",
    "Plafond",
    "Riferim.",
    "ND ori.",
]

ROW_FORMAT = [
    "object",
    "datetime64[ns]",
    "object",
    "datetime64[ns]",
    "object",
    "object",
    "object",
    "float64",
    "object",
    "object",
    "object",
    "object"
]

HEAD_FORMAT = [
    "float64",
    "float64", #int
    "float64", #int
    "float64", #int
    "float64",
    "datetime64[ns]",
    "object",
    "object",
]


END_ROW = "ND ori."
END_DOC = "Totali"


def cast2format(x, format):

    if format == 'object':
        try:
            return str(int(x))
        except ValueError:
            return str(x)
    if format == 'int': return int(x)
    try:
        if format == 'float64': return float(x)
    except ValueError as e:
        return str(x)

    try:
        if format == 'datetime64[ns]': return xlrd.xldate_as_datetime(x,0)
    except TypeError as e:
        return str(x)
    return str(x)


def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip().lstrip('0') if isinstance(x, str) else x
    return df.applymap(trim_strings)


def parse_data(sheet, current_row_id):
    stop = False
    row = []
    iva = []

    while not stop:
        #print('print', sheet.cell_value(current_row_id, 0))
        if sheet.cell_value(current_row_id, 0) == END_DOC:
            current_row_id += 1
            stop = True

        # end row
        if sheet.cell_value(current_row_id, 0) == END_ROW:
            # get ND ori. id
            header = cast2format(sheet.row_values(current_row_id)[DOC_ID],DOC_ID_FORMAT)
            current_row_id += 1
            iva = []
            while current_row_id < sheet.nrows and sheet.cell_value(current_row_id, HEADER_IDS[0]) != EMPTY_CELL:
                # get other values
                iva.append([cast2format(sheet.row_values(current_row_id)[i], f) for i,f in zip(HEADER_IDS, HEAD_FORMAT)])
                current_row_id += 1
            # reset row
            for i in range(len(row)):
                row[i].append(header)
            for i in range(len(iva)):
                iva[i].append(header)

            stop = True
        else:
            row.append([cast2format(sheet.row_values(current_row_id)[i], f) for i, f in zip(ROW_IDS, ROW_FORMAT)])

        if not stop:
            current_row_id += 1

    return row, iva, current_row_id


def clean_df(in_df):
    out_df = in_df.copy()
    out_df = trim_all_columns(out_df)
    out_df.replace('', pd.NA, inplace=True)
    out_df.ffill(axis=0, inplace=True)
    out_df.reset_index(inplace=True, drop=True)
    return out_df


def read_Accountingxlsx_to_df(path_to_xlsx):

    workbook = xlrd.open_workbook(path_to_xlsx)

    sheet = workbook.sheet_by_index(0)
    # df = pd.DataFrame(columns=list(HEAD_FORMAT.keys()) + list(ROW_FORMAT.keys()))

    # df_row = pd.DataFrame(columns=list(HEAD_FORMAT.keys()) + list(ROW_FORMAT.keys()))
    row_id = 1

    df_row = pd.DataFrame({k: pd.Series([], dtype=v) for k, v in zip(ROW_LABEL, ROW_FORMAT)})
    df_iva = pd.DataFrame({k: pd.Series([], dtype=v) for k, v in zip(HEAD_LABEL, HEAD_FORMAT)})
    while row_id < sheet.nrows - 1:
        row, iva, row_id = parse_data(sheet, row_id)
        try:
            df_row = df_row.append(pd.DataFrame(data=row, columns=ROW_LABEL))
        except ValueError:
            pass

        try:
            df_iva = df_iva.append(pd.DataFrame(data=iva, columns=HEAD_LABEL))
        except  ValueError:
            pass

    df_row = clean_df(df_row)
    df_iva = clean_df(df_iva)

    return df_row, df_iva


if __name__ == '__main__':

    path_to_file = r'../data/primanota/all.xlsx'
    df_row, df_iva = read_Accountingxlsx_to_df(path_to_file)
    save_path_iva = r'..\result\df_iva.xlsx'
    save_path_row = r'..\result\df_row.xlsx'

    df_row.to_excel(save_path_row)
    df_iva.to_excel(save_path_iva)
    print(df_row.shape, df_iva.shape)
    print(df_row.head())
    print(df_iva.head())