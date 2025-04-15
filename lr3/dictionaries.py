import pandas as pd
import json
import xml.etree.ElementTree as ET
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
data_dir = BASE_DIR / "files" / "original" / "view"

def load_dictionaries():
    # Словарь DOR_ID → название дороги
    with open(data_dir / "dim_dor_output.json", encoding='utf-8') as f:
        dor_data = json.load(f)
        dor_dict = {str(row["DOR_ID"]).zfill(3): row["NAME"] for row in dor_data}

    # Словарь VAR_ID → название показателя
    tree = ET.parse(data_dir / "dim_vars.xml")
    root = tree.getroot()
    vars_dict = {
        row.find("VAR_ID").text.strip(): row.find("NAME").text.strip()
        for row in root.findall("row")
    }

    # Словарь OPERIOD → описание (если нужно)
    operiod = pd.read_csv(data_dir / "dim_operiod.csv", sep=",", encoding="utf-8")
    operiod_dict = operiod.set_index("NAME").to_dict(orient="index")

    return dor_dict, vars_dict, operiod_dict
