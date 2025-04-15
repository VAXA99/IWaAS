import fastavro
import json
from pathlib import Path

# === Базовая директория проекта ===
BASE_DIR = Path(__file__).resolve().parent

# === Пути ===
input_path = BASE_DIR / "files" / "original" / "dim_dor.avro"
output_path = BASE_DIR / "files" / "original" / "view" / "dim_dor_output.json"
output_path.parent.mkdir(parents=True, exist_ok=True)

# === Чтение Avro и сохранение в JSON ===
with open(input_path, "rb") as f:
    reader = fastavro.reader(f)
    records = list(reader)

with open(output_path, "w", encoding="utf-8") as out_file:
    json.dump(records, out_file, ensure_ascii=False, indent=4)

print(f"[✓] Avro-файл успешно сохранён как JSON в: {output_path}")
