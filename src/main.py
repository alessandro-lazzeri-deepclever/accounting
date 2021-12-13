import pandas as pd

if __name__ == '__main__':

    if False:
        # read prima nota
        path2file = r'C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\all.xlsx'
        df_sack, df_iva = read_Accountingxlsx_to_df(path2file)

    else:
        # import prima nota
        df_sack = pd.read_pickle('../df_row.pkl')
        df_iva = pd.read_pickle('../df_iva.pkl')


    if False:
        # read fatture
        rootdir = Path(r'C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\16.03.2020-31.12.2020\Fatture in Cloud')

        files = []
        expand_folder(rootdir, files)
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
        df.to_pickle('df.pkl')
        df.to_excel('fatture.xlsx')
    else:
        # import fatture
        df = pd.read_pickle('../df.pkl')

    print("righe in prima nota",df_sack.shape)
    print("righe fatture", df.shape)
    if False:
        # match fatture and prima nota
        match_df(df, df_sack,debug=True)
        df.to_pickle('df_row_with_sack_index.pkl')
        print("righe in prima nota",df_sack.shape)
        print("righe fatture", df.shape)

    if False:
        df_row_with_sack_index = pd.read_pickle('df_row_with_sack_index.pkl')
        df_final = pd.merge(df_row_with_sack_index,df_sack,how="left", left_on="join_idx",right_index=True)
        df_final.to_pickle('df_final.pkl')
        df_final.to_excel('df_final.xlsx')
        print(df_final.isna().sum())
        #for c in df_final.columns:
        #    print(c)


    if True:
        df_row_with_sack_index = pd.read_pickle('../df_row_with_sack_index.pkl')

        for i, row in df_row_with_sack_index.iterrows():
            if row['join_idx'] not in df_sack.index:
                print(i)

