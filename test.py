import pandas as pd
import sqlite3

# Подключение к базе данных
conn = sqlite3.connect('C:/Users/Fnasdmjosmofisa/Desktop/project/crypto_data.db')

# Загрузка данных и преобразование
df = pd.read_sql('SELECT symbol, daily_return FROM returns WHERE daily_return IS NOT NULL', conn)
pivot_df = df.pivot_table(values='daily_return', index='date', columns='symbol', aggfunc='mean').dropna()

# Расчет корреляционной матрицы
correlation_matrix = pivot_df.corr()

# Вывод и сохранение
print(correlation_matrix)
correlation_matrix.to_csv('correlation_matrix.csv')

# Закрытие соединения
conn.close()