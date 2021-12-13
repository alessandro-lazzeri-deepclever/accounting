import xmlschema
from pathlib import Path


if __name__ == '__main__':
    path_to_root_with_xml_ti_validate = Path(r"C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\genn-marzo 2021\Fatture in Cloud\acquisti elettronici\03_marzo")
    path_to_schema = r"C:\Users\Utente\PycharmProjects\accounting\data\prime\Schema_VFPR12.xsd"
    #pathtoxml= r"C:\Users\Utente\Documents\progetti\prime office\nuovi dati\dati\UP DOWN COFFEE SRL\FATTURE XML\genn-marzo 2021\Fatture in Cloud\acquisti elettronici\03_marzo\IT00970110326_00B1W.xml"

    for path_to_xml in path_to_root_with_xml_ti_validate.iterdir():

        xsd = xmlschema.XMLSchema(path_to_schema)

        val = xsd.is_valid(str(path_to_xml))

        if val:
            print(path_to_xml.stem + ' is Valid! :)')
        else:
            print(path_to_xml.stem + ' is Not valid! :(')