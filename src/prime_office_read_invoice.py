from src.invoice_parser import *
from pathlib import Path


def expand_folder(path, file_list):
    if path.is_file():
        file_list.append(path)
    else:
        for p in path.iterdir():
            expand_folder(p,file_list)


if __name__ == '__main__':

    root = Path(r"../data/fatture")

    files = []
    expand_folder(root, files)

    for f in files:
        try:
            i = parse_invoice(f, columns_base)
            print(i['IdCodice_0'])
        except Exception as e:
            print(f.stem,e)

    path = Path(r"..\data\fatture\05_maggio\IT04839740489_s7f1.xml")

    invoice_xml = parse_invoice(path, columns_base)
    for k, v in invoice_xml.items():
        print(k, v)