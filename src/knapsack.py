import pandas as pd
import numpy as np
import itertools
from tqdm import tqdm
import time

def root_mean_suqared_percentage_error(y_true,y_pred):
    return (np.sqrt(np.mean(np.square((y_true - y_pred) / y_true)))) * 100

def random_knapsack(rows, sack, tolerance = 0.01, debug = False):
    """

    :param rows: one row per detailed row invoice
    :param sack: one row per aggregated
    :return: solutions list of tuples with binary matrix with the assignments and matrix
    """

    res = np.zeros(sack.shape)

    while not root_mean_suqared_percentage_error(sack,res) < tolerance:

        z = np.zeros(shape = (len(rows), len(sack)-1))
        z = np.array(np.append(z, np.ones(shape=(len(rows), 1)), axis=1))

        for i in z:
            np.random.shuffle(i)

        res = np.sum(z * np.expand_dims(rows,axis=1), axis=0)
        if debug:
            print("Mean % error:", root_mean_suqared_percentage_error(sack,res))
            print("rows", rows)
            print("sack", sack)
            print("z", z)
            input("press something")

    if debug:
        print("Mean % error:", root_mean_suqared_percentage_error(sack, res))
    return [(z,res)]

def generate_Z(row, cols):

    possible_rows = np.eye(cols)

    Z_idx = itertools.product(range(cols),repeat=row)

    for z_idx in Z_idx:
        z = []
        for i in z_idx:
            z.append(possible_rows[i])

        yield z

def brute_knapsack(rows, sack, tolerance = 0.01, debug = False, return_first = True, maxTime = 10):
    """

    :param rows: one row per detailed row invoice
    :param sack: one row per aggregated
    :return: solutions list of tuples with binary matrix with the assignments and matrix
    """
    solutions = []

    res = np.zeros(sack.shape)

    startTime = time.time()

    for z in tqdm(generate_Z(len(rows), len(sack)), total=len(sack)**len(rows)):
        currentTimeTime = time.time()
        if currentTimeTime - startTime > maxTime:
            break

        res = np.sum(z * np.expand_dims(rows,axis=1), axis=0)
        solutions.append((z,res))
        if return_first and root_mean_suqared_percentage_error(sack, res) < tolerance:
            return solutions

        if debug:
            print("Mean % error:", root_mean_suqared_percentage_error(sack,res))
            print("rows", rows)
            print("sack", sack)
            print("z", z)
            #input("press something")

    if debug:
        print("Mean % error:", root_mean_suqared_percentage_error(sack, res))
    return solutions


def match_df(df_row, df_sack, debug = False):

    # invoice list
    row_invoices = df_row["FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Numero"].unique()
    sack_invoices = df_sack["ND ori."].unique()

    invoiceIds = np.intersect1d(row_invoices,sack_invoices)

    for invoiceID in invoiceIds:
        rows = df_row[df_row["FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Numero"] == invoiceID]
        sack = df_sack[(df_sack["ND ori."] == invoiceID) & (df_sack["D/A"] == "D") & (
                    df_sack["Rag. Sociale / Descrizione"] != "IVA SU ACQUISTI")]

        row_vals = np.array(rows["FatturaElettronicaBody_DatiBeniServizi_DettaglioLinee_PrezzoTotale"].to_numpy(),
                            dtype=np.float32)

        sack_vals = sack["Importo"].to_numpy()

        if debug:
            print(invoiceID, len(invoiceIds))

            print("rows", rows[["FatturaElettronicaBody_DatiBeniServizi_DettaglioLinee_NumeroLinea",
                                "FatturaElettronicaBody_DatiBeniServizi_DettaglioLinee_Descrizione",
                                "FatturaElettronicaBody_DatiBeniServizi_DettaglioLinee_PrezzoTotale"
                                ]])
            print("sack", sack)
            #input("press something")

        solutions = brute_knapsack(row_vals, sack_vals, debug=False, return_first=True)
        z = solutions[-1]
        print(z)
        for idx, z_val in zip(rows.index, z[0]):
            df_row.at[idx, "join_idx"] = sack.index[np.argwhere(z_val)[0][0]]



def test_knapsack():
    rows = np.array([5, 3, 4, 1, 6])

    sack = np.array([5 + 3 + 1, 4 + 6])
    res = [0, 0]

    while not np.equal(sack, res).all():
        z = np.random.binomial(n=1, p=.5, size=len(rows))
        z = np.array([z, 1 - z])

        res = np.sum(z * rows, axis=1)

    print(z, sack, res)

if __name__ == '__main__':

    if True:

        df1 = pd.DataFrame([[5,"a"], [3,"b"], [4,"c"], [1,"d"], [6,"e"]], columns=["val","feat"], index = np.arange(1,10,2))

        df2 = pd.DataFrame([[9,"aa"], [10,"bb"]], columns=["val","feat"], index = np.arange(1,10,5))

        z = brute_knapsack(df1["val"].to_numpy(), df2["val"].to_numpy(), debug=False, return_first=False)

        print(z)
        print(z[-1])
        target = np.array([9,10])
        for i,j in z:
            if (j == target).all():
                print(i,j)



    if False:
        df_sack = pd.read_pickle('df_row.pkl')

        #df_iva = pd.read_pickle('df_iva.pkl')
        df_row = pd.read_pickle('df.pkl')
        print(df_sack.shape)
        print(df_row.shape)
        match_df(df_row, df_sack)

        print(df_row.shape)


    if False:
        path2file = r'C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\all.xlsx'
        df_row, df_iva = read_Accountingxlsx_to_df(path2file)
        df_row.to_pickle('df_row.pkl')
        df_iva.to_pickle('df_iva.pkl')

        df_row.from_pickle('df_row.pkl')
        df_iva.from_pickle('df_iva.pkl')
        df.from_pickle('df.pkl')

        rootdir = r'C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\16.03.2020-31.12.2020\Fatture in Cloud\acquisti elettronici\05_maggio'

        invoicedir = Path(rootdir)
        df = make_df(invoicedir)
        df.to_pickle('df.pkl')

    if False:
        df_row = pd.read_pickle('df_row.pkl')
        df_iva = pd.read_pickle('df_iva.pkl')
        df = pd.read_pickle('df.pkl')


        invoiceIds = df["FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Numero"].unique()

        for invoiceID in invoiceIds:
            if invoiceID in ['0000759','0000764','0000747']:
                pass
            else:
                print(invoiceID)
                rows = df[df["FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Numero"] == invoiceID]
                sack = df_row[(df_row["ND ori."] == invoiceID) & (df_row["D/A"] == "D") & (df_row["Rag. Sociale / Descrizione"] != "IVA SU ACQUISTI")]

                row_vals = np.array(rows["FatturaElettronicaBody_DatiBeniServizi_DettaglioLinee_PrezzoTotale"].to_numpy(),dtype=np.float32)
                print(row_vals, np.sum(row_vals))
                print(sack)
                sack_vals = sack["Importo"].to_numpy()
                print(sack_vals)
                print(knapsack(row_vals,sack_vals, debug=True))


        if False:

            #df_join = pd.merge(df_row, df, left_on="ND ori.", right_on="FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Numero")
            df_join = pd.merge(df,df_row,  right_on="ND ori.", left_on="FatturaElettronicaBody_DatiGenerali_DatiGeneraliDocumento_Numero")

            df_join.to_excel('join.xlsx')
            print(df_join.head())
            print(df_join.columns)