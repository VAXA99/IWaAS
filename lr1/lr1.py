import pandas as pd
import re
from pathlib import Path

# Определяем базовую директорию проекта
BASE_DIR = Path(__file__).resolve().parent

# Пути к файлам
txt_file_path = BASE_DIR / "files" / "original" / "clients"
excel_file_path = BASE_DIR / "files" / "original" / "master_tasks.xlsx"
json_file_path = BASE_DIR / "files" / "original" / "master-emps.json"

# Пути для сохранения Parquet-файлов
parquet_dir = BASE_DIR / "files" / "processed"
parquet_dir.mkdir(parents=True, exist_ok=True)
txt_parquet_path = parquet_dir / "clients.parquet"
excel_parquet_path = parquet_dir / "master_tasks.parquet"
json_parquet_path = parquet_dir / "master_emps.parquet"

# Чтение clients
def read_txt_fixed_width(file_path):
    # Читаем файл построчно
    raw_lines = file_path.read_text(encoding="utf-8").splitlines()
    cleaned_rows = []

    for line in raw_lines:
        if "FULL_CLIENT_NAME" in line or "varchar" in line:
            continue

        # Строгое разбиение по позициям с учётом символов
        full_name = line[0:34].strip()
        passport = line[34:44].strip()
        license = line[44:50].strip()
        vin = line[52:69].strip()

        cleaned_rows.append({
            "FULL_CLIENT_NAME": full_name,
            "PASSPORT": passport,
            "LICENSE": license,
            "VIN": vin
        })

    return pd.DataFrame(cleaned_rows)



# Чтение всех листов Excel
def read_excel_all_sheets(file_path):
    sheets = pd.read_excel(file_path, sheet_name=None, dtype=str)
    all_data = []

    print("Excel: найдено листов:", len(sheets))

    for sheet_name, df in sheets.items():
        print(f"\n==== Содержимое листа: {sheet_name} ====\n")
        print(df)

        if "Цена ремонта" in df.columns:
            df["Цена ремонта"] = df["Цена ремонта"].str.extract(r'(\d+)')
            df["Цена ремонта"] = pd.to_numeric(df["Цена ремонта"], errors="coerce")

        df["date"] = sheet_name
        all_data.append(df)

    return pd.concat(all_data, ignore_index=True)

# Удаление сокращений из строк
def remove_abbreviations(text):
    if isinstance(text, str):
        words = text.split()
        words = [word for word in words if not re.search(r'\.', word)]
        return ' '.join(words)
    return text

# Чтение и очистка JSON
def read_and_fix_json(file_path):
    df = pd.read_json(file_path)
    for col in ["last_name", "first_name", "second_name"]:
        df[col] = df[col].apply(remove_abbreviations)
    return df

# Чтение файлов
txt_df = read_txt_fixed_width(txt_file_path)
excel_df = read_excel_all_sheets(excel_file_path)
json_df = read_and_fix_json(json_file_path)

# Приведение типов
txt_df = txt_df.astype(str)
excel_df = excel_df.astype(str)
json_df = json_df.astype(str)

# Сохраняем в Parquet
txt_df.to_parquet(txt_parquet_path, index=False, engine="fastparquet")
excel_df.to_parquet(excel_parquet_path, index=False, engine="fastparquet")
json_df.to_parquet(json_parquet_path, index=False, engine="fastparquet")

# Подтверждение
print(f"\nТекстовый файл сохранен в: {txt_parquet_path}")
print(f"Excel файл сохранен в: {excel_parquet_path}")
print(f"JSON файл сохранен в: {json_parquet_path}")
