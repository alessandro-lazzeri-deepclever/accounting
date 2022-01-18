import pandas as pd
from reader_primanota import read_Accountingxlsx_to_df
from pathlib import Path
from prime_office_read_invoice import expand_folder
import xml
import xml.etree.ElementTree as ET
from reader_fattura import Fattura
from knapsack import match_df


if __name__ == '__main__':
    #input

    #primanota e fatture
    path_primanota = r'..\data\primanota\all.xlsx'
    rootpath_fatture = Path(r'..\data\fatture\05_maggio')

    #output
    path_iva = Path(r'..\result\df_iva.pkl')
    path_row = Path(r'..\result\df_row.pkl')

    path_fatture = Path(r'..\result\df.pkl')
    path_fatture_xlsx = Path(r'..\result\fatture.xlsx')

    path_fatture_and_primanotaindex = Path(r'..\result\df_row_with_sack_index.pkl')
    path_fatture_and_primanotaindex_xlsx = Path(r'..\result\df_row_with_sack_index.xlsx')

    path_dataset = Path(r'../result/df_final.pkl')
    path_dataset_xlsx = Path(r'../result/df_final.xlsx')

    if not (path_iva.is_file() and path_row.is_file()):
        # read prima nota

        df_sack, df_iva = read_Accountingxlsx_to_df(path_primanota)

        df_sack.to_pickle(path_row)
        df_iva.to_pickle(path_iva)

    else:
        # import prima nota
        df_sack = pd.read_pickle(path_row)
        df_iva = pd.read_pickle(path_iva)

    if not (path_fatture.is_file() and path_fatture_xlsx.is_file()):
        # read fatture

        files = []
        expand_folder(rootpath_fatture, files)
        print(len(files))

        parsed_invoices = []
        for invoice_path in files:
            try:
                invoice = ET.parse(invoice_path)
                root = invoice.getroot()

                parsed_invoices.extend(Fattura(root))
            except xml.etree.ElementTree.ParseError:
                print("Error ", invoice_path)

        df = pd.DataFrame(parsed_invoices)
        df.to_pickle(path_fatture)
        df.to_excel(path_fatture_xlsx)
    else:
        # import fatture
        df = pd.read_pickle(path_fatture)

    print("righe in prima nota",df_sack.shape)
    print("righe fatture", df.shape)

    # match fatture and prima nota
    match_df(df, df_sack,debug=True)
    df.to_pickle(path_fatture_and_primanotaindex)
    df.to_excel(path_fatture_and_primanotaindex_xlsx)
    print("righe in prima nota",df_sack.shape)
    print("righe fatture", df.shape)

    if path_fatture_and_primanotaindex.is_file():
        df_row_with_sack_index = pd.read_pickle(path_fatture_and_primanotaindex)
        df_final = pd.merge(df_row_with_sack_index,df_sack,how="left", left_on="join_idx",right_index=True)
        df_final.to_pickle(path_dataset)
        df_final.to_excel(path_dataset_xlsx)
        print(df_final.isna().sum())
        #for c in df_final.columns:
        #    print(c)


