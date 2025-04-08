import pandas as pd
import re
from pathlib import Path
import os

# === Пути ===
BASE_DIR = Path(__file__).resolve().parent
txt_path = BASE_DIR / "files" / "original" / "clients"
json_path = BASE_DIR / "files" / "original" / "master-emps.json"
excel_path = BASE_DIR / "files" / "original" / "master_tasks.xlsx"
output_excel_path = BASE_DIR / "files" / "processed" / "etl_result.xlsx"
Path(output_excel_path).parent.mkdir(parents=True, exist_ok=True)

# === Утилиты ===
def remove_abbreviations(text):
    if isinstance(text, str):
        words = text.split()
        return ' '.join([w for w in words if not re.search(r'\.', w)])
    return text

def read_txt_fixed_width(path):
    df = pd.read_fwf(path, header=0)
    df.columns = ["FULL_CLIENT_NAME", "PASSPORT", "VIN"]
    return df

def read_and_fix_json(path):
    df = pd.read_json(path)
    for col in ["last_name", "first_name", "second_name"]:
        df[col] = df[col].apply(remove_abbreviations)
    return df

def read_excel_all_sheets(path):
    sheets = pd.read_excel(path, sheet_name=None, dtype=str)
    all_data = []
    for name, df in sheets.items():
        df.columns = df.columns.str.strip()
        df = df.rename(columns={
            "Мастер": "master",
            "ВИН": "vin",
            "Операция": "operation",
            "кол-часов": "number_of_hours",
            "Цена ремонта": "price_of_repair",
            "Год": "manufacture_year",
            "Производитель": "manufacturer",
            "Модель": "model"
        })
        if "price_of_repair" in df.columns:
            df["price_of_repair"] = df["price_of_repair"].str.replace(r"[^\d]", "", regex=True)
            df["price_of_repair"] = pd.to_numeric(df["price_of_repair"], errors="coerce")
        df["date"] = name
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True)

# === Загрузка данных ===
txt_df = read_txt_fixed_width(txt_path)
json_df = read_and_fix_json(json_path)
excel_df = read_excel_all_sheets(excel_path)

# === DIM_Мастер ===
dim_master = json_df.rename(columns={
    "emp_num": "emp_num",
    "last_name": "фамилия",
    "first_name": "имя",
    "second_name": "отчество",
    "coeff": "коэффициент"
})[["emp_num", "фамилия", "имя", "отчество", "коэффициент"]].drop_duplicates()

# === DIM_Клиент (ID от 1, паспорт как str) ===
fio_split = txt_df["FULL_CLIENT_NAME"].str.strip().str.split(" ", expand=True)
fio_split.columns = ["фамилия", "имя", "отчество"]

dim_client = pd.concat([txt_df["PASSPORT"], fio_split], axis=1)
dim_client = dim_client.rename(columns={"PASSPORT": "паспорт"}).drop_duplicates().reset_index(drop=True)
dim_client.insert(0, "id", dim_client.index + 1)
dim_client["паспорт"] = dim_client["паспорт"].astype(str)

client_id_map = dim_client.set_index("паспорт")["id"].to_dict()

# === DIM_Производитель ===
dim_manufacturer = excel_df[["manufacturer"]].drop_duplicates().reset_index(drop=True)
dim_manufacturer["id"] = dim_manufacturer.index + 1
dim_manufacturer = dim_manufacturer.rename(columns={"manufacturer": "наименование"})
manufacturer_lookup = dim_manufacturer.set_index("наименование")["id"].to_dict()

# === DIM_Модель ===
dim_model = excel_df[["model", "manufacturer"]].drop_duplicates().reset_index(drop=True)
dim_model["производитель"] = dim_model["manufacturer"].map(manufacturer_lookup)
dim_model["id"] = dim_model.index + 1
dim_model = dim_model.rename(columns={"model": "наименование"})
model_lookup = dim_model.set_index("наименование")["id"].to_dict()

# === DIM_Автомобиль ===
dim_car = excel_df[["vin", "manufacture_year", "model"]].drop_duplicates()
dim_car["модель"] = dim_car["model"].map(model_lookup)
dim_car = dim_car.rename(columns={"vin": "vin", "manufacture_year": "год_выпуска"})
dim_car = dim_car[["vin", "год_выпуска", "модель"]].drop_duplicates()

# === DIM_Операция ===
dim_operation = excel_df[["operation"]].drop_duplicates().reset_index(drop=True)
dim_operation["id"] = dim_operation.index + 1
dim_operation = dim_operation.rename(columns={"operation": "наименование"})
operation_lookup = dim_operation.set_index("наименование")["id"].to_dict()

# === FACT_Заказ ===
fact = excel_df.copy()
fact["id_операция"] = fact["operation"].map(operation_lookup)
fact["vin_автомобиля"] = fact["vin"]

# Привязка мастера
fact["master_clean"] = fact["master"].apply(remove_abbreviations)
fact = fact.merge(json_df[["emp_num", "last_name"]].rename(columns={"last_name": "master_last"}),
                  left_on="master_clean", right_on="master_last", how="left")

# Привязка клиента (паспорт как str)
fact = fact.merge(txt_df[["PASSPORT", "VIN"]], left_on="vin", right_on="VIN", how="left")
fact["PASSPORT"] = fact["PASSPORT"].astype(str)
fact["id_клиент"] = fact["PASSPORT"].map(client_id_map)

fact = fact.rename(columns={
    "number_of_hours": "кол-во часов",
    "price_of_repair": "цена ремонта",
    "date": "дата"
})

fact_order = fact[["id_клиент", "id_операция", "emp_num", "vin_автомобиля", "дата", "кол-во часов", "цена ремонта"]]

# === ExcelWriter ===
with pd.ExcelWriter(output_excel_path, engine="xlsxwriter") as writer:
    dim_master.to_excel(writer, sheet_name="DIM_Мастер", index=False)
    dim_client.to_excel(writer, sheet_name="DIM_Клиент", index=False)
    dim_manufacturer.to_excel(writer, sheet_name="DIM_Производитель", index=False)
    dim_model[["id", "наименование", "производитель"]].to_excel(writer, sheet_name="DIM_Модель", index=False)
    dim_car.to_excel(writer, sheet_name="DIM_Автомобиль", index=False)
    dim_operation.to_excel(writer, sheet_name="DIM_Операция", index=False)
    fact_order.to_excel(writer, sheet_name="FACT_Заказ", index=False)

    # Excel форматирование для id_клиент
    worksheet = writer.sheets["FACT_Заказ"]
    workbook = writer.book
    text_fmt = workbook.add_format({'num_format': '0'})
    worksheet.set_column('A:A', 12, text_fmt)

print(f"\n✅ Готово! Excel-файл со всеми таблицами сохранён в:\n{output_excel_path}")
