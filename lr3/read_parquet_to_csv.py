import pandas as pd
from pathlib import Path

# === Базовая директория проекта ===
BASE_DIR = Path(__file__).resolve().parent

# === Пути ===
input_path = BASE_DIR / "files" / "original" / "base.parquet"
output_path = BASE_DIR / "files" / "original" / "view" / "base_parquet_output.csv"
output_path.parent.mkdir(parents=True, exist_ok=True)

# === Чтение Parquet и сохранение в CSV ===
df = pd.read_parquet(input_path)
df.to_csv(output_path, index=False)

print(f"[✓] Parquet-файл успешно сохранён как CSV в: {output_path}")
