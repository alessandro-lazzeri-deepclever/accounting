from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET
import re
import numpy as np
pd.set_option('display.max_columns', None)
#xls_structure_path = Path(r'C:\Users\Utente\Documents\progetti\prime office\fattura digitale\Rappresentazione tabellare fattura ordinaria_13032020.xls')

#df = pd.read_excel(xls_structure_path, usecols="A:F,N", nrows=270)


def getFirst(x):
    return x[0]


AGGREGATOR_OPERATOR = {
    'first' : getFirst,
    'mean' : np.mean,
    'max' : np.max,
    'min' : np.min,
    'count': len
}

AGGREGATOR = {
    'DatiRitenuta' : {
        'TipoRitenuta' : ['first'],
        'ImportoRitenuta' : ['first'],
        'AliquotaRitenuta' : ['first'],
        'CausalePagamento' : ['first']
    },

    'DatiCassaPrevidenziale' : {
        'TipoCassa' : ['first'],
        'AlCassa' : ['first'],
        'ImportoContributoCassa' : ['first'],
        'ImponibileCassa' : ['first'],
        'AliquotaIVA' : ['first'],
        'Ritenuta' : ['first'],
        'Natura' : ['first'],
        'RiferimentoAmministrazione' : ['first']
    },

    'DatiGeneraliDocumento' :{
        'Causale' : ['first']
    },
    'DatiOrdineAcquisto' : {
        'RiferimentoNumeroLinea' : ['first'],
        'IdDocumento' : ['first'],
        'Data' : ['first'],
        'NumItem' : ['first'],
        'CodiceCommessaConvenzione' : ['first'],
        'CodiceCUP' : ['first'],
        'CodiceCIG' : ['first']

    },
    'DatiGenerali' : {
        'DatiContratto' : ['first'],
        'DatiConvenzione' : ['first'],
        'DatiRicezione' : ['first'],
        'DatiFattureCollegate' : ['first']
    },
    'DatiSAL' : {
        'RiferimentoFase' : ['first']
    },
    'DatiDDT' : {
        'RiferimentoNumeroLinea' : ['first']
    },
    'CodiceArticolo' : {
        'CodiceTipo' : ['first'],
        'CodiceValore' : ['first']
    },
    'ScontoMaggiorazione' : {
        'Tipo' : ['first'],
        'Percentuale' : ['first'],
        'Importo' : ['first']
    },
    'AltriDatiGestionali' : {
        'TipoDato' : ['first'],
        'RiferimentoTesto' : ['first'],
        'RiferimentoNumero' : ['first'],
        'RiferimentoData' : ['first']
    },
    'DatiRiepilogo' : {
        'AliquotaIVA' : ['first'],
        'Natura' : ['first'],
        'SpeseAccessorie' : ['first'],
        'Arrotondamento' : ['first'],
        'ImponibileImporto' : ['first'],
        'Imposta' : ['first'],
        'EsigibilitaIVA' : ['first'],
        'RiferimentoNormativo' : ['first']
    },
    'DettaglioPagamento' : {
        'Beneficiario' : ['first'],
        'ModalitaPagamento' : ['first'],
        'DataRiferimentoTerminiPagamento' : ['first'],
        'GiorniTerminiPagamento' : ['first'],
        'DataScadenzaPagamento' : ['first'],
        'ImportoPagamento' : ['first'],
        'CodUfficioPostale' : ['first'],
        'CognomeQuietanzante' : ['first'],
        'NomeQuietanzante' : ['first'],
        'CFQuietanzante' : ['first'],
        'TitoloQuietanzante' : ['first'],
        'IstitutoFinanziario' : ['first'],
        'ABI' : ['first'],
        'CAB' : ['first'],
        'BIC' : ['first'],
        'ScontoPagamentoAnticipato' : ['first'],
        'DataLimitePagamentoAnticipato' : ['first'],
        'PenalitaPagamentiRitardati' : ['first'],
        'DataDecorrenzaPenale' : ['first'],
        'CodicePagamento' : ['first']
    },
    'Allegati' : {
        'NomeAttachment' : ['first'],
        'AlgoritmoCompressione' : ['first'],
        'FormatoAttachment' : ['first'],
        'DescrizioneAttachment' : ['first'],
        'Attachment' : ['first']
    }
}


def aggregateDatiPagamento(lines):
    return lines[0]

def aggregateDatiDDT(lines):
    return lines[0]


def get_tag(field):
    try:
        return re.search('<(.*?)>', field).group(1)
    except TypeError:
        pass
    except AttributeError:
        pass


def rename_dict(dict, tag):
    new_dict = {}
    for k, v in dict.items():
        new_dict[tag+'_'+k] = v
    return new_dict


def Fattura(invoice):
    """

    :param invoice: root of the invoice (etree)
    :return: dataframe: one row per row in the invoice (DettaglioLinee)
    """
    parsed_invoices = []

    # There is only one FatturaElettronicaHeader
    dict_FatturaElettronicaHeader = getFatturaElettronicaHeader(invoice, 'FatturaElettronicaHeader')

    # There are 1+ body and each body may have 1+ row
    list_FatturaElettronicaBody = getFatturaElettronicaBody(invoice, 'FatturaElettronicaBody')

    for row in list_FatturaElettronicaBody:

        parsed_invoices.append(row | dict_FatturaElettronicaHeader)

    return parsed_invoices



def getFatturaElettronicaBody(invoice, tag):
    """
    :param invoice: root of the getFatturaElettronicaBody
    :return: list of the rows of all the invoices
    """
    invoices = invoice.findall(tag) # list

    lines = []

    for i, invoice in enumerate(invoices):

        DatiGenerali = getDatiGenerali(invoice, 'DatiGenerali')
        DatiBeniServizi = getDatiBeniServizi(invoice, 'DatiBeniServizi')  # list
        DatiVeicoli = getDatiVeicoli(invoice, 'DatiVeicoli')
        DatiPagamento = getDatiPagamento(invoice, 'DatiPagamento')
        Allegati = getAggregatedValuesFromRoot(invoice, 'Allegati')

        for j, line in enumerate(DatiBeniServizi):

            l = line | DatiGenerali | DatiVeicoli | DatiPagamento | Allegati

            lines.append(l)

    return [rename_dict(r, tag) for r in lines]


def getDatiGenerali(invoice, tag):
    invoice = invoice.find(tag)

    DatiGeneraliDocumento = getDatiGeneraliDocumento(invoice, 'DatiGeneraliDocumento')

    DatiOrdineAcquisto = getAggregatedValuesFromRoot(invoice, 'DatiOrdineAcquisto')
    DatiContrattoConvenzioneRicezioneFattureCollegate = getAggregatedValuesFromRoot(invoice, 'DatiGenerali')
    DatiSAL = getAggregatedValuesFromRoot(invoice, 'DatiSAL')
    DatiDDT = getDatiDDT(invoice, 'DatiDDT')
    DatiTrasporto = getDatiTrasporto(invoice, 'DatiTrasporto')
    FatturaPrincipale = getFatturaPrincipale(invoice, 'FatturaPrincipale')

    return rename_dict(DatiGeneraliDocumento | DatiOrdineAcquisto | DatiContrattoConvenzioneRicezioneFattureCollegate |
                       DatiSAL | DatiDDT | DatiTrasporto | FatturaPrincipale, tag)


def getDatiDDT(invoice, tag):
    invoices = invoice.findall(tag)

    if len(invoices) == 0:
        RiferimentoNumeroLinea = getAggregatedValuesFromRoot(invoice, 'DatiDDT')

        DatiDDT = RiferimentoNumeroLinea | {
            'NumeroDDT': '',
            'DataDDT': ''
        }

        for k,aggs in AGGREGATOR[tag].items():
            for agg in aggs:
                DatiDDT[k+agg] = ''
    else:
        lines = []

        for i, line in enumerate(invoices):

            try:
                NumeroDDT = line.find('NumeroDDT').text
            except AttributeError:
                NumeroDDT = ''

            try:
                DataDDT = line.find('DataDDT').text
            except AttributeError:
                DataDDT = ''

            RiferimentoNumeroLinea = getAggregatedValuesFromRoot(line, 'DatiDDT')

            l = RiferimentoNumeroLinea | {
                'NumeroDDT': NumeroDDT,
                'DataDDT': DataDDT,
            }

            lines.append(l)

        DatiDDT = aggregateDatiDDT(lines)

    return rename_dict(DatiDDT, tag)

def getDatiGeneraliDocumento(invoice, tag):
    invoice = invoice.find(tag)

    TipoDocumento = invoice.find('TipoDocumento').text
    Divisa = invoice.find('Divisa').text
    Data = pd.to_datetime(invoice.find('Data').text)
    Numero = invoice.find('Numero').text.lstrip('0')

    DatiRitenuta = getAggregatedValuesFromRoot(invoice, 'DatiRitenuta') #dict

    DatiBollo = getDatiBollo(invoice, 'DatiBollo')

    DatiCassaPrevidenziale = getAggregatedValuesFromRoot(invoice, 'DatiCassaPrevidenziale')

    ScontoMaggiorazione = getAggregatedValuesFromRoot(invoice, 'ScontoMaggiorazione')

    try:
        Causali = invoice.findall('Causale').text
        Causale = {}

        for field, aggs in AGGREGATOR[tag].items():
            values = []
            for causale in Causali:
                values.append(causale.find(field).text)
            for agg in aggs:
                Causale[field + agg] = AGGREGATOR_OPERATOR[agg](values)

    except AttributeError:
        Causale = ''

    try:
        ImportoTotaleDocumento = np.float64(invoice.find('ImportoTotaleDocumento').text)
    except AttributeError:
        ImportoTotaleDocumento = np.float64(0)

    try:
        Arrotondamento = invoice.find('Arrotondamento').text
    except AttributeError:
        Arrotondamento = ''

    try:
        Art73 = invoice.find('Art73').text
    except AttributeError:
        Art73 = ''

    return rename_dict(DatiRitenuta | DatiBollo | DatiCassaPrevidenziale | ScontoMaggiorazione | {
        'Causale' : Causale,
        'TipoDocumento' : TipoDocumento,
        'Divisa' : Divisa,
        'Data' : Data,
        'Numero' : Numero,
        'ImportoTotaleDocumento': ImportoTotaleDocumento,
        'Arrotondamento': Arrotondamento,
        'Art73' : Art73
    }, tag)


def getAggregatedValuesFromRoot(invoice, tag):
    invoices = invoice.findall(tag)

    result = {}

    for field, aggs in AGGREGATOR[tag].items():
        values = []
        if len(invoices) == 0:
            values.append('')
        else:
            for inv in invoices:
                try:
                    values.append(inv.find(field).text)
                except AttributeError:
                    values.append('')
        for agg in aggs:
            result[field+agg] = AGGREGATOR_OPERATOR[agg](values)

    return rename_dict(result, tag)


def getDatiRitenuta(invoice, tag):
    invoices = invoice.findall(tag)

    result = {}

    for field, aggs in AGGREGATOR[tag].items():
        values = []
        for inv in invoices:
            values.append(inv.find(field).text)
        for agg in aggs:
            result[field+agg] = AGGREGATOR_OPERATOR[agg](values)

    return rename_dict(result, tag)


def getDatiBollo(invoice, tag):
    invoice = invoice.find(tag)

    try:
        BolloVirtuale = invoice.find('BolloVirtuale').text
    except AttributeError:
        BolloVirtuale = ''

    try:
        ImportoBollo = invoice.find('ImportoBollo').text
    except AttributeError:
        ImportoBollo = ''

    return rename_dict({
        'BolloVirtuale': BolloVirtuale,
        'ImportoBollo': ImportoBollo,
    }, tag)


def getDatiTrasporto(invoice, tag):
    invoice = invoice.find(tag)
    DatiAnagraficiVettore = getDatiAnagraficiVettore(invoice, 'DatiAnagraficiVettore')

    try:
        MezzoTrasporto = invoice.find('MezzoTrasporto').text
    except AttributeError:
        MezzoTrasporto = ''

    try:
        CausaleTrasporto = invoice.find('CausaleTrasporto').text
    except AttributeError:
        CausaleTrasporto = ''

    try:
        NumeroColli = invoice.find('NumeroColli').text
    except AttributeError:
        NumeroColli = ''

    try:
        Descrizione = invoice.find('Descrizione').text
    except AttributeError:
        Descrizione = ''

    try:
        UnitaMisuraPeso = invoice.find('UnitaMisuraPeso').text
    except AttributeError:
        UnitaMisuraPeso = ''

    try:
        PesoLordo = invoice.find('PesoLordo').text
    except AttributeError:
        PesoLordo = ''

    try:
        PesoNetto = invoice.find('PesoNetto').text
    except AttributeError:
        PesoNetto = ''

    try:
        DataOraRitiro = invoice.find('DataOraRitiro').text
    except AttributeError:
        DataOraRitiro = ''

    try:
        DataInizioTrasporto = invoice.find('DataInizioTrasporto').text
    except AttributeError:
        DataInizioTrasporto = ''

    try:
        TipoResa = invoice.find('TipoResa').text
    except AttributeError:
        TipoResa = ''

    IndirizzoResa = getSede(invoice, 'IndirizzoResa')

    try:
        DataOraConsegna = invoice.find('DataOraConsegna').text
    except AttributeError:
        DataOraConsegna = ''

    return rename_dict(DatiAnagraficiVettore | IndirizzoResa | {
        'MezzoTrasporto': MezzoTrasporto,
        'CausaleTrasporto': CausaleTrasporto,
        'NumeroColli': NumeroColli,
        'Descrizione': Descrizione,
        'UnitaMisuraPeso': UnitaMisuraPeso,
        'PesoLordo': PesoLordo,
        'PesoNetto': PesoNetto,
        'DataOraRitiro': DataOraRitiro,
        'DataInizioTrasporto': DataInizioTrasporto,
        'TipoResa': TipoResa,
        'DataOraConsegna' : DataOraConsegna
    },tag)


def getDatiAnagraficiVettore(invoice, tag):
    try:
        invoice = invoice.find(tag)
    except AttributeError:
        pass

    IdFiscaleIVA = getIdFiscaleIVA(invoice, 'DatiAnagraficiVettore')

    try:
        CodiceFiscale = invoice.find('CodiceFiscale').text
    except AttributeError:
        CodiceFiscale = ''

    Anagrafica = getAnagrafica(invoice, 'Anagrafica')

    try:
        NumeroLicenzaGuida = invoice.find('NumeroLicenzaGuida').text
    except AttributeError:
        NumeroLicenzaGuida = ''

    return rename_dict(IdFiscaleIVA | Anagrafica | {
        'CodiceFiscale': CodiceFiscale,
        'NumeroLicenzaGuida': NumeroLicenzaGuida,
    }, tag)


def getFatturaPrincipale(invoice, tag):
    invoice = invoice.find(tag)
    try:
        NumeroFatturaPrincipale = invoice.find('NumeroFatturaPrincipale').text
    except AttributeError:
        NumeroFatturaPrincipale = ''


    try:
        DataFatturaPrincipale = invoice.find('DataFatturaPrincipale').text
    except AttributeError:
        DataFatturaPrincipale = ''

    return rename_dict({
        'NumeroFatturaPrincipale': NumeroFatturaPrincipale,
        'DataFatturaPrincipale': DataFatturaPrincipale,
    }, tag)


def getDatiBeniServizi(invoice, tag):
    invoice = invoice.find(tag)
    DettaglioLinee = getDettaglioLinee(invoice, 'DettaglioLinee') # list

    DatiRiepilogo = getAggregatedValuesFromRoot(invoice, 'DatiRiepilogo')


    return [rename_dict(r | DatiRiepilogo, tag) for r in DettaglioLinee]


def getDettaglioLinee(invoice, tag):
    invoices = invoice.findall(tag) # list of all lines

    lines = []

    for i,line in enumerate(invoices):

        CodiceArticolo = getAggregatedValuesFromRoot(line, 'CodiceArticolo')

        ScontoMaggiorazione = getAggregatedValuesFromRoot(line, 'ScontoMaggiorazione')

        AltriDatiGestionali = getAggregatedValuesFromRoot(line, 'AltriDatiGestionali')

        try:
            NumeroLinea = line.find('NumeroLinea').text
        except AttributeError:
            NumeroLinea = ''

        try:
            TipoCessionePrestazione = line.find('TipoCessionePrestazione').text
        except AttributeError:
            TipoCessionePrestazione = ''

        try:
            Descrizione = line.find('Descrizione').text
        except AttributeError:
            Descrizione = ''

        try:
            Quantita = line.find('Quantita').text
        except AttributeError:
            Quantita = ''

        try:
            UnitaMisura = line.find('UnitaMisura').text
        except AttributeError:
            UnitaMisura = ''

        try:
            DataInizioPeriodo = line.find('DataInizioPeriodo').text
        except AttributeError:
            DataInizioPeriodo = ''

        try:
            DataFinePeriodo = line.find('DataFinePeriodo').text
        except AttributeError:
            DataFinePeriodo = ''

        try:
            PrezzoUnitario = line.find('PrezzoUnitario').text
        except AttributeError:
            PrezzoUnitario = ''

        try:
            PrezzoTotale = line.find('PrezzoTotale').text
        except AttributeError:
            PrezzoTotale = ''

        try:
            AliquotaIVA = line.find('AliquotaIVA').text
        except AttributeError:
            AliquotaIVA = ''

        try:
            Ritenuta = line.find('Ritenuta').text
        except AttributeError:
            Ritenuta = ''

        try:
            Natura = line.find('Natura').text
        except AttributeError:
            Natura = ''

        try:
            RiferimentoAmministrazione = line.find('RiferimentoAmministrazione').text
        except AttributeError:
            RiferimentoAmministrazione = ''

        l = CodiceArticolo | ScontoMaggiorazione | AltriDatiGestionali | {
            'NumeroLinea': NumeroLinea,
            'TipoCessionePrestazione': TipoCessionePrestazione,
            'Descrizione': Descrizione,
            'Quantita': Quantita,
            'UnitaMisura': UnitaMisura,
            'DataInizioPeriodo': DataInizioPeriodo,
            'DataFinePeriodo': DataFinePeriodo,
            'PrezzoUnitario': PrezzoUnitario,
            'PrezzoTotale': PrezzoTotale,
            'AliquotaIVA': AliquotaIVA,
            'Ritenuta': Ritenuta,
            'Natura': Natura,
            'RiferimentoAmministrazione': RiferimentoAmministrazione
            }

        lines.append(l)

    return [rename_dict(r, tag) for r in lines]


def getDatiVeicoli(invoice, tag):
    invoice = invoice.find(tag)

    try:
        Data = invoice.find('Data').text
    except AttributeError:
        Data = ''

    try:
        TotalePercorso = invoice.find('TotalePercorso').text
    except AttributeError:
        TotalePercorso = ''

    return rename_dict({
        'Data': Data,
        'TotalePercorso': TotalePercorso,
    }, tag)


def getDatiPagamento(invoice, tag):
    invoices = invoice.findall(tag)

    if len(invoices) == 0:

        DettaglioPagamento = getAggregatedValuesFromRoot(invoice, 'DettaglioPagamento')
        lines = [DettaglioPagamento | {
                'CondizioniPagamento': ''
            }]
    else:
        lines = []

        for i, line in enumerate(invoices):

            DettaglioPagamento = getAggregatedValuesFromRoot(line, 'DettaglioPagamento')

            try:
                CondizioniPagamento = line.find('CondizioniPagamento').text
            except AttributeError:
                CondizioniPagamento = ''

            l = DettaglioPagamento | {
                'CondizioniPagamento': CondizioniPagamento
            }

            lines.append(l)

    DatiPagamento = aggregateDatiPagamento(lines)

    return rename_dict(DatiPagamento, tag)


def getFatturaElettronicaHeader(invoice, tag):
    invoice = invoice.find(tag)
    DatiTrasmissione = getDatiTrasmissione(invoice, 'DatiTrasmissione')

    CedentePrestatore = getCedentePrestatore(invoice, 'CedentePrestatore')

    RappresentanteFiscale = getRappresentanteFiscale(invoice, 'RappresentanteFiscale')

    CessionarioCommittente = getCessionarioCommittente(invoice, 'CessionarioCommittente')

    TerzoIntermediarioOSoggettoEmittente = getTerzoIntermediarioOSoggettoEmittente(invoice, 'TerzoIntermediarioOSoggettoEmittente')

    try:
        SoggettoEmittente = invoice.find('SoggettoEmittente').text
    except AttributeError:
        SoggettoEmittente = ''

    return rename_dict(DatiTrasmissione | CedentePrestatore | RappresentanteFiscale | CessionarioCommittente | TerzoIntermediarioOSoggettoEmittente | {
        'SoggettoEmittente': SoggettoEmittente
    }, tag)


def getDatiTrasmissione(invoice, tag):
    invoice = invoice.find(tag)
    IdTrasmittente = getIdTrasmittente(invoice, 'IdTrasmittente')

    ProgressivoInvio = invoice.find('ProgressivoInvio').text
    FormatoTrasmissione = invoice.find('FormatoTrasmissione').text
    CodiceDestinatario = invoice.find('CodiceDestinatario').text

    ContattiTrasmittente = getContattiTrasmittente(invoice, 'ContattiTrasmittente')

    try:
        PECDestinatario = invoice.find('PECDestinatario').text
    except AttributeError:
        PECDestinatario = ''

    return rename_dict(IdTrasmittente | ContattiTrasmittente | {
        'ProgressivoInvio' : ProgressivoInvio,
        'FormatoTrasmissione' : FormatoTrasmissione,
        'CodiceDestinatario' : CodiceDestinatario,
        'PECDestinatario' : PECDestinatario
    }, tag)


def getCedentePrestatore(invoice, tag):
    invoice = invoice.find(tag)
    DatiAnagrafici = getDatiAnagrafici(invoice, 'DatiAnagrafici')

    Sede = getSede(invoice, 'Sede')

    StabileOrganizzazione = getStabileOrganizzazione(invoice, 'StabileOrganizzazione')

    IscrizioneREA = getIscrizioneREA(invoice, 'IscrizioneREA')

    Contatti = getContatti(invoice, 'Contatti')

    RiferimentoAmministrazione = invoice.find('RiferimentoAmministrazione')

    return rename_dict(DatiAnagrafici | Sede | StabileOrganizzazione | IscrizioneREA | Contatti | {
        'RiferimentoAmministrazione' : RiferimentoAmministrazione
    }, tag)

"""
def DatiAnagrafici(invoice):
    IdFiscaleIVA =  getIdFiscaleIVA(invoice, 'IdFiscaleIVA')

    CodiceFiscale =invoice.find('CodiceFiscale')
    Anagrafica = getAnagrafica(invoice, 'Anagrafica')

    AlboProfessionale = invoice.find('AlboProfessionale').text
    ProvinciaAlbo = invoice.find('ProvinciaAlbo').text
    NumeroIscrizioneAlbo = invoice.find('NumeroIscrizioneAlbo').text
    DataIscrizioneAlbo = invoice.find('DataIscrizioneAlbo').text
    RegimeFiscale = invoice.find('RegimeFiscale').text

    return IdFiscaleIVA | Anagrafica | {
        'CodiceFiscale' : CodiceFiscale,
        'AlboProfessionale': AlboProfessionale,
        'ProvinciaAlbo': ProvinciaAlbo,
        'NumeroIscrizioneAlbo': NumeroIscrizioneAlbo,
        'DataIscrizioneAlbo': DataIscrizioneAlbo,
        'RegimeFiscale': RegimeFiscale
    }
"""
def getIdFiscaleIVA(invoice, tag):
    try:
        invoice = invoice.find(tag)
    except AttributeError:
        return rename_dict({
        'IdPaese': '',
        'IdCodice': ''
    }, tag)

    IdPaese = invoice.find('IdPaese').text
    IdCodice = invoice.find('IdCodice').text

    return rename_dict({
        'IdPaese': IdPaese,
        'IdCodice': IdCodice
    }, tag)



def getAnagrafica(invoice, tag):
    try:
        invoice = invoice.find(tag)
    except AttributeError:
        return rename_dict({
        'Denominazione': '',
        'Nome': '',
        'Cognome': '',
        'Titolo': '',
        'CodEORI': ''
    }, tag)

    try:
        Denominazione = invoice.find('Denominazione').text
    except AttributeError:
        Denominazione = ''
    try:
        Nome = invoice.find('Nome').text
    except AttributeError:
        Nome = ''
    try:
        Cognome = invoice.find('Cognome').text
    except AttributeError:
        Cognome = ''
    try:
        Titolo = invoice.find('Titolo').text
    except AttributeError:
        Titolo = ''

    try:
        CodEORI = invoice.find('CodEORI').text
    except AttributeError:
        CodEORI = ''

    return rename_dict({
        'Denominazione': Denominazione,
        'Nome': Nome,
        'Cognome': Cognome,
        'Titolo': Titolo,
        'CodEORI': CodEORI
    }, tag)

"""
def AlboProfessionale():
def ProvinciaAlbo():
def NumeroIscrizioneAlbo():
def DataIscrizioneAlbo():
def RegimeFiscale():


def IdPaese():
def IdCodice():
def Denominazione():
def Nome():
def Cognome():
def Titolo():
def CodEORI():
"""

def getSede(invoice, tag):
    try:
        invoice = invoice.find(tag)
    except AttributeError:
        return rename_dict({
            'Indirizzo': '',
            'NumeroCivico': '',
            'CAP': '',
            'Comune': '',
            'Provincia': '',
            'Nazione': ''
        }, tag)

    try:
        Indirizzo = invoice.find('Indirizzo').text
    except AttributeError:
        Indirizzo = ''

    try:
        NumeroCivico = invoice.find('NumeroCivico').text
    except AttributeError:
        NumeroCivico = ''

    try:
        CAP = invoice.find('CAP').text
    except AttributeError:
        CAP = ''

    try:
        Comune = invoice.find('Comune').text
    except AttributeError:
        Comune = ''

    try:
        Provincia = invoice.find('Provincia').text
    except AttributeError:
        Provincia = ''

    try:
        Nazione = invoice.find('Nazione').text
    except AttributeError:
        Nazione = ''

    return rename_dict({
        'Indirizzo': Indirizzo,
        'NumeroCivico': NumeroCivico,
        'CAP': CAP,
        'Comune': Comune,
        'Provincia': Provincia,
        'Nazione': Nazione
    },tag)


def getStabileOrganizzazione(invoice, tag):
    invoice = invoice.find(tag)

    try:
        Indirizzo = invoice.find('Indirizzo').text
    except AttributeError:
        Indirizzo = ''

    try:
        NumeroCivico = invoice.find('NumeroCivico').text
    except AttributeError:
        NumeroCivico = ''

    try:
        Comune = invoice.find('Comune').text
    except AttributeError:
        Comune = ''

    try:
        Provincia = invoice.find('Provincia').text
    except AttributeError:
        Provincia = ''

    try:
        CAP = invoice.find('CAP').text
    except AttributeError:
        CAP = ''

    try:
        Nazione = invoice.find('Nazione').text
    except AttributeError:
        Nazione = ''

    return rename_dict({
        'Indirizzo': Indirizzo,
        'NumeroCivico': NumeroCivico,
        'CAP': CAP,
        'Comune': Comune,
        'Provincia': Provincia,
        'Nazione': Nazione
    },tag)


def getIscrizioneREA(invoice, tag):
    invoice = invoice.find(tag)

    try:
        Ufficio = invoice.find('Ufficio').text
    except AttributeError:
        Ufficio = ''

    try:
        NumeroREA = invoice.find('NumeroREA').text
    except AttributeError:
        NumeroREA = ''

    try:
        CapitaleSociale = invoice.find('CapitaleSociale').text
    except AttributeError:
        CapitaleSociale = ''

    try:
        SocioUnico = invoice.find('SocioUnico').text
    except AttributeError:
        SocioUnico = ''

    try:
        StatoLiquidazione = invoice.find('StatoLiquidazione').text
    except AttributeError:
        StatoLiquidazione = ''

    return rename_dict({
        'Ufficio': Ufficio,
        'NumeroREA': NumeroREA,
        'CapitaleSociale': CapitaleSociale,
        'SocioUnico': SocioUnico,
        'StatoLiquidazione': StatoLiquidazione,
    },tag)


def getContatti(invoice, tag):
    invoice = invoice.find(tag)

    try:
        Telefono = invoice.find('Telefono').text
    except AttributeError:
        Telefono = ''

    try:
        Fax = invoice.find('Fax').text
    except AttributeError:
        Fax = ''

    try:
        Email = invoice.find('Email').text
    except AttributeError:
        Email = ''

    return rename_dict({
        'Telefono': Telefono,
        'Fax': Fax,
        'Email': Email
    },tag)


def getRappresentanteFiscale(invoice, tag):
    invoice = invoice.find(tag)

    return rename_dict(getDatiAnagrafici(invoice,'DatiAnagrafici'), tag)

def getDatiAnagrafici(invoice, tag):
    try:
        invoice = invoice.find(tag)
    except AttributeError:
        invoice = None

    IdFiscaleIVA = getIdFiscaleIVA(invoice, 'IdFiscaleIVA')

    try:
        CodiceFiscale = invoice.find('CodiceFiscale').text
    except AttributeError:
        CodiceFiscale = ''

    Anagrafica = getAnagrafica(invoice, 'Anagrafica')

    try:
        AlboProfessionale = invoice.find('AlboProfessionale').text
    except AttributeError:
        AlboProfessionale = ''

    try:
        ProvinciaAlbo = invoice.find('ProvinciaAlbo').text
    except AttributeError:
        ProvinciaAlbo = ''

    try:
        NumeroIscrizioneAlbo = invoice.find('NumeroIscrizioneAlbo').text
    except AttributeError:
        NumeroIscrizioneAlbo = ''

    try:
        DataIscrizioneAlbo = invoice.find('DataIscrizioneAlbo').text
    except AttributeError:
        DataIscrizioneAlbo = ''

    try:
        RegimeFiscale = invoice.find('RegimeFiscale').text
    except AttributeError:
        RegimeFiscale = ''

    return rename_dict(IdFiscaleIVA | Anagrafica |{
        'CodiceFiscale' : CodiceFiscale,
        'AlboProfessionale' : AlboProfessionale,
        'ProvinciaAlbo' : ProvinciaAlbo,
        'NumeroIscrizioneAlbo' : NumeroIscrizioneAlbo,
        'DataIscrizioneAlbo' : DataIscrizioneAlbo,
        'RegimeFiscale' : RegimeFiscale
    },tag)


def getCessionarioCommittente(invoice, tag):
    invoice = invoice.find(tag)
    DatiAnagrafici = getDatiAnagrafici(invoice, 'DatiAnagrafici')

    Sede = getSede(invoice, 'Sede')

    StabileOrganizzazione = getStabileOrganizzazione(invoice, 'StabileOrganizzazione')

    RappresentanteFiscale = getRappresentanteFiscale2(invoice, 'RappresentanteFiscale')

    return rename_dict(DatiAnagrafici | Sede | StabileOrganizzazione | RappresentanteFiscale, tag)

def getRappresentanteFiscale2(invoice, tag):
    invoice = invoice.find(tag)
    IdFiscaleIVA = getIdFiscaleIVA(invoice, 'IdFiscaleIVA')

    try:
        Denominazione = invoice.find('Denominazione').text
    except AttributeError:
        Denominazione = ''
    try:
        Nome = invoice.find('Nome').text
    except AttributeError:
        Nome = ''
    try:
        Cognome = invoice.find('Cognome').text
    except AttributeError:
        Cognome = ''


    return rename_dict(IdFiscaleIVA | {
        'Denominazione' : Denominazione,
        'Nome' : Nome,
        'Cognome' : Cognome
    },tag)


def getTerzoIntermediarioOSoggettoEmittente(invoice, tag):
    invoice = invoice.find(tag)
    DatiAnagrafici = getDatiAnagrafici(invoice, 'DatiAnagrafici')
    return rename_dict(DatiAnagrafici, tag)

def getIdTrasmittente(invoice, tag):
    invoice = invoice.find(tag)
    IdPaese = invoice.find('IdPaese').text
    IdCodice = invoice.find('IdCodice').text

    return rename_dict({
        'IdPaese' : IdPaese,
        'IdCodice' : IdCodice
    },tag)


def getContattiTrasmittente(invoice, tag):

    invoice = invoice.find(tag)
    try:
        Telefono = invoice.find('Telefono').text
    except AttributeError:
        Telefono = ''

    try:
        Email = invoice.find('Email').text
    except AttributeError:
        Email = ''

    return rename_dict({
        'Telefono': Telefono,
        'Email': Email
    },tag)

def format_structure():
    col_range = np.arange(6)

    columns = df.columns

    df_processed = pd.DataFrame()
    for i in col_range:
        t = df[columns[i]].apply(get_tag).dropna()
        # t.columns[0] = 'l_%s' % i

        df_processed = pd.concat([df_processed, t])

        df_processed.columns = ['l_%s' % j for j in range(i + 1)]

    print(df_processed.sort_index())

    df_processed.to_excel(Path(r'C:\Users\Utente\Documents\progetti\prime office\fattura digitale\struttura.xlsx'))

def make_df(rootdir):
    from src.invoice_parser import get_file_paths
    invoicedir = Path(rootdir)

    invoice_paths = get_file_paths(invoicedir, False)
    parsed_invoices = []
    for invoice_path in invoice_paths:
        invoice = ET.parse(invoice_path)
        root = invoice.getroot()

        parsed_invoices.extend(Fattura(root))

    df = pd.DataFrame(parsed_invoices)
    return df


if __name__ == '__main__':
    path_to_invoice = Path(r"/data/prime/fatture/FT XML GIUGNO/CAMPAJOLA 236.xml")

    invoice = ET.parse(path_to_invoice)
    root = invoice.getroot()

    """
    #for k,v in getFatturaElettronicaHeader(root,'FatturaElettronicaHeader').items():
    #    print(k,v)

    total_element = 0
    for i,row in enumerate(getFatturaElettronicaBody(root,'FatturaElettronicaBody')):
        for k,v in row.items():
            total_element += 1
            print(i,k,v)
    """
    from src.invoice_parser import get_file_paths

    rootdir = r'C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\16.03.2020-31.12.2020\Fatture in Cloud\acquisti elettronici'

    df1 = make_df(rootdir)

    rootdir2 = r'C:\Users\Utente\PycharmProjects\accounting\data\prime\fatture'

    df2 = make_df(rootdir2)

    c1 = df1.columns
    c2 = df2.columns

    print(df1.shape)

    print(df2.shape)

    diff = list(set(c1)-set(c2))
    for i in diff:
        print(i)

    for c in df1.columns:
        print(c)