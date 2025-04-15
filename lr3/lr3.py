import pandas as pd
import fastavro
import xml.etree.ElementTree as ET
from pathlib import Path

# === Пути ===
BASE_DIR = Path(__file__).resolve().parent
original_dir = BASE_DIR / "files" / "original"
processed_dir = BASE_DIR / "files" / "processed"
processed_dir.mkdir(parents=True, exist_ok=True)

# === Чтение данных ===
base = pd.read_parquet(original_dir / "base.parquet")
base["VAR_ID"] = base["VAR_ID"].astype(str).str.strip()
base["DOR_ID"] = base["DOR_ID"].astype(str).str.zfill(3)

# === DOR словарь из .avro ===
with open(original_dir / "dim_dor.avro", "rb") as f:
    reader = fastavro.reader(f)
    dor_data = list(reader)
    dor_dict = {
        str(row["DOR_ID"]).zfill(3): row["NAME"]
        for row in dor_data
    }

# === VAR словарь из XML ===
tree = ET.parse(original_dir / "dim_vars.xml")
root = tree.getroot()
vars_dict = {
    row.find("VAR_ID").text.strip(): row.find("NAME").text.strip()
    for row in root.findall("row")
}

# === Подготовка ===
base["DOR_NAME"] = base["DOR_ID"].map(dor_dict)
base["VAR_NAME"] = base["VAR_ID"].map(vars_dict)
base["DATE"] = pd.to_datetime(base["DATE"])
base["Year"] = base["DATE"].dt.year

# === Отчёт 4: Списочный отчёт по VAR_ID=4110 (₽, накопит.) ===
df_4110 = base[(base["OPERIOD"] == "H") & (base["VAR_ID"] == "4110")].copy()
report_4 = df_4110.groupby(["Year", "DOR_NAME"])["fact"].sum().reset_index()
report_4["Cumulative_Sum"] = report_4.groupby("DOR_NAME")["fact"].cumsum()
report_4["Formatted_Value"] = report_4["Cumulative_Sum"].apply(lambda x: f"{x:,.2f} ₽")

# === Отчёт 5: Статистика по тому же показателю ===
report_5 = df_4110.groupby("Year", as_index=False)["fact"].mean().rename(columns={"fact": "Average_Fact"}).sort_values("Year")

# === Отчёт 6: Сводный по VAR_ID=[17050, 17070, 17090] (ед., накопит.) ===
summary_vars = ["17050", "17070", "17090"]
df_summary = base[(base["OPERIOD"] == "H") & (base["VAR_ID"].isin(summary_vars))].copy()
df_summary["Cumulative_Sum"] = df_summary.groupby(["VAR_ID", "DOR_NAME"])["fact"].cumsum()
df_summary["Formatted_Value"] = df_summary["Cumulative_Sum"].apply(lambda x: f"{x:,.2f} ед.")
df_summary["VAR_NAME"] = df_summary["VAR_ID"].map(vars_dict)

report_6 = df_summary.pivot_table(
    index=["DOR_NAME", "VAR_NAME"],
    columns="Year",
    values="Cumulative_Sum",
    aggfunc="sum"
).fillna(0).reset_index()

# === Отчёт 7: График по VAR_ID=[200047, 200048] (2003 год, P, без накоп.) ===
graph_vars = ["200047", "200048"]
df_graph = base[
    (base["OPERIOD"] == "P") &
    (base["VAR_ID"].isin(graph_vars)) &
    (base["DATE"].dt.year == 2003)
].copy()

df_graph_grouped = df_graph.groupby(["DOR_NAME", "VAR_ID"])["fact"].sum().reset_index()
report_7 = df_graph_grouped.pivot_table(
    index="DOR_NAME",
    columns="VAR_ID",
    values="fact",
    aggfunc="sum"
).fillna(0).reset_index()

report_7.rename(columns={v: vars_dict.get(v, v) for v in graph_vars}, inplace=True)

# === Экспорт в Excel с графиком ===
output_excel = processed_dir / "report.xlsx"

with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
    report_4.to_excel(writer, sheet_name="Списочный отчёт", index=False)
    report_5.to_excel(writer, sheet_name="Стат. отчёт", index=False)
    report_6.to_excel(writer, sheet_name="Сводный отчёт", index=False)
    report_7.to_excel(writer, sheet_name="График (данные)", index=False)

    workbook = writer.book
    worksheet = writer.sheets["График (данные)"]
    chart = workbook.add_chart({'type': 'column'})

    categories = ['График (данные)', 1, 0, len(report_7), 0]
    series_1 = ['График (данные)', 1, 1, len(report_7), 1]
    series_2 = ['График (данные)', 1, 2, len(report_7), 2]

    chart.add_series({
        'name': list(report_7.columns)[1],
        'categories': categories,
        'values': series_1,
        'fill': {'color': 'blue'}
    })
    chart.add_series({
        'name': list(report_7.columns)[2],
        'categories': categories,
        'values': series_2,
        'fill': {'color': 'red'}
    })

    chart.set_x_axis({'name': 'Дороги'})
    chart.set_y_axis({'name': 'Значение показателя (ед.)'})
    chart.set_title({'name': 'График неисправного парка, 2003'})
    chart.set_legend({'position': 'bottom'})

    chart_sheet = workbook.add_worksheet("График")
    chart_sheet.insert_chart("B2", chart)

print(f"[✓] Отчёт успешно сформирован: {output_excel}")
