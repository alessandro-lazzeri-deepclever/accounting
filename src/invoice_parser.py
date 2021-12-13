from pathlib import Path
import xml.etree.ElementTree as ET
import os
import pandas as pd
import a38.fattura as a38


def read_invoices_to_dataframe(path, columns_lines, columns_base, debug = False):

    df = pd.DataFrame()

    invoice_paths = get_file_paths(path,debug)

    if debug:
        print(path, invoice_paths)

    for invoice_path in invoice_paths:
        invoice = ET.parse(invoice_path)
        root = invoice.getroot()
        invoice_df = invoice_to_dataframe(root, columns_lines, columns_base)
        df = pd.concat([df,invoice_df],axis=0)
    # TODO: assign type to each column properly
    df['FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data'] = pd.to_datetime(df['FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data'])
    return df.reset_index(drop=True)


def get_file_paths(invoicedir, debug = False):
    res = []
    for subdir, dirs, files in os.walk(invoicedir):
        if debug:
            print(subdir,dirs,files)
        for file in files:
            res.append(os.path.join(subdir, file))
    return res


def parse_invoice(path, columns):
    res = {}
    invoice = ET.parse(path)
    root = invoice.getroot()
    for col in columns:
        for i, child in enumerate(root.findall(col)):
            res['%s_%s' %(child.tag,i)] = child.text
    return res


def get_line(root, columns):

    lines_list = []
    for col in columns:
        for i, child in enumerate(root.findall(col)):
            if len(lines_list) >= i+1:
                lines_list[i][col] = child.text
            else:
                lines_list.append({col : child.text})

    return pd.DataFrame(lines_list)


def get_base(root, columns):
    tmp = {}
    for col in columns:
        child = root.find(col)
        if child is not None:
            tmp[col] = [child.text]
        else:
            tmp[col] = 'NA'
    return pd.DataFrame(tmp)


def invoice_to_dataframe(root, columns_lines, columns_base):
    lines = get_line(root, columns_lines)
    base = get_base(root, columns_base)
    lines[columns_base] = base[columns_base].values[0]
    return lines


def import_invoice(path):
    # read invoice in path and return a38.fattura
    f = a38.Fattura()

    r = ET.parse(path).getroot()
    f.from_etree(r)
    return f


columns_base = ['FatturaElettronicaHeader/CedentePrestatore/DatiAnagrafici/Anagrafica/Denominazione',
                'FatturaElettronicaHeader/CedentePrestatore/Sede/CAP',
                'FatturaElettronicaHeader/CessionarioCommittente/DatiAnagrafici/Anagrafica/Denominazione',
                'FatturaElettronicaHeader/CessionarioCommittente/Sede/CAP',
                'FatturaElettronicaHeader/DatiTrasmissione/IdTrasmittente/IdCodice',
                'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data',
                'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/ImportoTotaleDocumento',

                'FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Numero',
                'FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/AliquotaIVA',
                'FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/ImponibileImporto',
                'FatturaElettronicaBody/DatiBeniServizi/DatiRiepilogo/Imposta'
                ]

columns_lines = [
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/NumeroLinea',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/TipoCessionePrestazione',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/CodiceArticolo',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/CodiceArticolo/CodiceTipo',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/CodiceArticolo/CodiceValore',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/Descrizione',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/Quantita',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/UnitaMisura',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/DataInizioPeriodo',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/DataFinePeriodo',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/PrezzoUnitario',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/ScontoMaggiorazione',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/ScontoMaggiorazione/Tipo',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/ScontoMaggiorazione/Percentuale',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/ScontoMaggiorazione/Importo',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/PrezzoTotale',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AliquotaIVA',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/Ritenuta',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/Natura',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/RiferimentoAmministrazione',
    #'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali',
    #'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali/TipoDato',
    #'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali/RiferimentoTesto',
    #'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali/RiferimentoNumero',
    #'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali/RiferimentoData',
]


altridatigestionali= {
    'head' : 'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali',
    'aggregatorMode' : 'first',
    'fields' : [
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali/TipoDato',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali/RiferimentoTesto',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali/RiferimentoNumero',
    'FatturaElettronicaBody/DatiBeniServizi/DettaglioLinee/AltriDatiGestionali/RiferimentoData'
    ]
}

def aggregator():
    pass

def xml2xls(infile, outfile):
    invoicedir = Path(infile)

    df = read_invoices_to_dataframe(invoicedir, columns_lines, columns_base)

    df.to_excel(outfile)


def expand(node, columns_base, columns_lines, df_base = [], df_line = [], path = '' ):
    path += node.tag
    if path in columns_base:
        df_base.append({path : node.text})
    if path in columns_lines:
        df_line.append({path : node.text})
    path +='/'
    path = path.replace('{http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2}FatturaElettronica/','')

    for child in node:
        df_base, df_line = expand(child, columns_base, columns_lines, df_base, df_line, path)
    return df_base, df_line

if __name__ == '__main__':
    if False:
        rootdir = r'C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\16.03.2020-31.12.2020\Fatture in Cloud\acquisti elettronici\05_maggio'

        xml2xls(rootdir, 'fatture.xlsx')

    if True:
        rootdir = Path(r'/data/prime')

        invoicedir = rootdir.joinpath('fatture')

        path = Path(r"/data/prime/fatture/FT XML GIUGNO/CAMPAJOLA 236.xml")

        invoice = ET.parse(path)
        root = invoice.getroot()

        print([r.tag for r in root])

        print(root.find('FatturaElettronicaHeader').find('DatiTrasmissione').find('ProgressivoInvio').text)
        #df_base, df_line = expand(root, columns_base, columns_lines)
        #print(df_base)
        #print(df_line)

    if False:
        invoice = import_invoice(path)
        print(path)
        print(invoice.fattura_elettronica_body)
        print('*'*50)
        print(invoice.fattura_elettronica_header.cedente_prestatore.dati_anagrafici.anagrafica.to_str())

        print(invoice.fattura_elettronica_header.cessionario_committente.dati_anagrafici.anagrafica.denominazione)
        for linea in invoice.fattura_elettronica_body[0].dati_beni_servizi.dettaglio_linee:
            print(linea)


    if False:
        for invoice_path in get_file_paths(root=rootdir,folder=invoicedir):
            invoice = import_invoice(invoice_path)
            print(invoice_path)
            print(invoice.fattura_elettronica_body)




