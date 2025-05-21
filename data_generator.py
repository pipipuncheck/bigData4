import os
import pandas as pd

# Папка с исходными CSV
input_folder = "initial_data"

# Получаем список всех файлов, начинающихся с MOCK_DATA
csv_files = sorted([f for f in os.listdir(input_folder) if f.startswith("MOCK_DATA") and f.endswith(".csv")])

# Читаем и объединяем все файлы
dataframes = []
for file in csv_files:
    path = os.path.join(input_folder, file)
    df = pd.read_csv(path)
    dataframes.append(df)

# Объединяем всё в один DataFrame
full_df = pd.concat(dataframes, ignore_index=True)

# Проверим, что в сумме 10000 строк
assert full_df.shape[0] == 10000, f"Ожидалось 10000 строк, получено {full_df.shape[0]}"

# Присваиваем уникальные ID
full_df['id'] = range(1, 10001)

# Разделим на две части
postgres_df = full_df.iloc[:5000].copy()
clickhouse_df = full_df.iloc[5000:].copy()

# Сохраняем в файлы
postgres_df.to_csv("postgres.csv", index=False)
clickhouse_df.to_csv("clickhouse.csv", index=False)

print("Файлы postgres.csv и clickhouse.csv успешно созданы!")
