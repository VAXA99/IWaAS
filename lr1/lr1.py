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
parquet_dir.mkdir(parents=True, exist_ok=True)  # Создаем директорию, если её нет
txt_parquet_path = parquet_dir / "clients.parquet"
excel_parquet_path = parquet_dir / "master_tasks.parquet"
json_parquet_path = parquet_dir / "master_emps.parquet"

# Функция для чтения текстового файла
def read_txt_fixed_width(file_path):
    return pd.read_fwf(file_path)

# Функция для чтения всех листов Excel с очисткой данных и добавлением колонки "date"
def read_excel_all_sheets(file_path):
    sheets = pd.read_excel(file_path, sheet_name=None, dtype=str)  # Принудительно читаем всё как строки
    all_data = []

    for sheet_name, df in sheets.items():
        if "Цена ремонта" in df.columns:
            df["Цена ремонта"] = df["Цена ремонта"].str.extract(r'(\d+)')  # Оставляем только числа
            df["Цена ремонта"] = pd.to_numeric(df["Цена ремонта"], errors="coerce")  # Преобразуем в int (NaN для ошибок)
        
        df["date"] = sheet_name  # Добавляем колонку с названием листа (дату)
        all_data.append(df)

    return pd.concat(all_data, ignore_index=True)  # Объединяем все листы в один DataFrame

# Функция для удаления сокращений (слов с точками)
def remove_abbreviations(text):
    if isinstance(text, str):  # Проверяем, что это строка
        words = text.split()  # Разбиваем строку на слова
        words = [word for word in words if not re.search(r'\.', word)]  # Удаляем слова с точками
        return ' '.join(words)  # Собираем обратно в строку
    return text  # Если не строка, оставляем как есть

# Функция для чтения и исправления JSON-файла
def read_and_fix_json(file_path):
    df = pd.read_json(file_path)

    # Применяем удаление сокращений ко всем нужным полям
    for col in ["last_name", "first_name", "second_name"]:
        df[col] = df[col].apply(remove_abbreviations)

    return df

# Чтение файлов
txt_df = read_txt_fixed_width(txt_file_path)
excel_df = read_excel_all_sheets(excel_file_path)
json_df = read_and_fix_json(json_file_path)

# Принудительно задаем все столбцы как строки (чтобы избежать ошибок)
txt_df = txt_df.astype(str)
excel_df = excel_df.astype(str)
json_df = json_df.astype(str)

# Сохранение в Parquet с engine="fastparquet"
txt_df.to_parquet(txt_parquet_path, index=False, engine="fastparquet")
excel_df.to_parquet(excel_parquet_path, index=False, engine="fastparquet")
json_df.to_parquet(json_parquet_path, index=False, engine="fastparquet")

# Вывод информации о сохранении
print(f"Текстовый файл сохранен в: {txt_parquet_path}")
print(f"Excel файл сохранен в: {excel_parquet_path}")
print(f"JSON файл сохранен в: {json_parquet_path}")
