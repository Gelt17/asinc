import pandas as pd
from datetime import datetime

def extract_date_from_filename(filename):
    """
    Извлекает дату из названия файла
    """
    try:
        date_str = filename.split('_')[-1].split('.')[0]

        return datetime.strptime(date_str[:8], '%Y%m%d')
        
    except ValueError as e:
        print(f"Ошибка преобразования даты из файла {filename}: {e}")
        return None

def add_string(file_path):
    search_text="Единица измерения: Метрическая тонна"
    db = {"Код Инструмента": [], 
        "Наименование Инструмента": [], 
        "Базис поставки": [],
        "Объем Договоров в единицах измерения": [],
        "Обьем Договоров, руб.": [],
        "Количество Договоров, шт.": []}

    row = 0

    df = pd.read_excel(file_path)

    for row_idx in range(len(df)):
        for col_idx in range(len(df.columns)):
            cell_value = df.iloc[row_idx, col_idx]
            if pd.notna(cell_value) and search_text in str(cell_value):
                print(f"✅ Найдено в файле {file_path}:")
                row = row_idx

    for idx in range(row + 3, len(df)):
        cell_value = df.iloc[idx, 14]
        try:
            if pd.notna(cell_value) and int(str(cell_value)) >= 1:
                db["Код Инструмента"].append(str(df.iloc[idx, 1]))
                db["Наименование Инструмента"].append(str(df.iloc[idx, 2]))
                db["Базис поставки"].append(str(df.iloc[idx, 3]))
                db["Объем Договоров в единицах измерения"].append(str(df.iloc[idx, 4]))
                db["Обьем Договоров, руб."].append(str(df.iloc[idx, 5]))
                db["Количество Договоров, шт."].append(int(str(df.iloc[idx, 14])))
        except Exception:
            continue

    return db
