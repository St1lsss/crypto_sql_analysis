
# Импортируем инструменты (библиотеки), которые нам нужны
import pandas as pd  # Для работы с таблицами
import yfinance as yf  # Для загрузки данных о S&P 500 и золоте
import requests  # Для загрузки данных о криптовалютах
import sqlite3  # Для создания базы данных
import plotly.express as px  # Для красивых графиков
import plotly.graph_objects as go  # Для графиков линий
import streamlit as st  # Для создания дашборда
from datetime import datetime, timedelta  # Для работы с датами

# Функция для загрузки данных о криптовалютах с сайта CoinGecko
def get_crypto_data(symbol, days=365):
    url = f"https://api.coingecko.com/api/v3/coins/{symbol}/market_chart?vs_currency=usd&days={days}"
    response = requests.get(url)  # Запрашиваем данные
    data = response.json()  # Преобразуем данные в удобный формат
    prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])  # Создаем таблицу
    prices['timestamp'] = pd.to_datetime(prices['timestamp'], unit='ms')  # Преобразуем время
    prices['symbol'] = symbol  # Добавляем название актива
    return prices

# Подключаемся к базе данных (создаст файл crypto_data.db)
conn = sqlite3.connect('crypto_data.db')
cursor = conn.cursor()

# Создаем таблицу для хранения цен, если её нет
cursor.execute('''
    CREATE TABLE IF NOT EXISTS prices (
        date TEXT,  
        symbol TEXT,  
        price REAL  
    )
''')
conn.commit()  # Сохраняем изменения

# Устанавливаем заголовок для дашборда
st.title("Анализ рынка криптовалют: анализ на основе SQL и Python")

# Определяем, какие активы будем анализировать
assets = {
    'Bitcoin': 'bitcoin',
    'Ethereum': 'ethereum',
    'S&P 500': '^GSPC',
    'Золото': 'GLD'
}

# Устанавливаем дату начала (год назад)
start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

# Загружаем и сохраняем данные в базу

# Загружаем и сохраняем данные в базу
cursor.execute('DROP TABLE IF EXISTS prices')
cursor.execute('DROP TABLE IF EXISTS returns')
conn.commit()
# Загружаем и сохраняем данные в базу
for name, ticker in assets.items():
    if 'bitcoin' in ticker or 'ethereum' in ticker:
        # Загружаем данные о криптовалютах
        df = get_crypto_data(ticker)
        df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')  # Форматируем дату
        df['symbol'] = name  # Добавляем название актива
        print(f"Columns in df for {name}: {df.columns.tolist()}")  # Отладка
        df[['date', 'symbol', 'price']].to_sql('prices', conn, if_exists='append', index=False)
    else:
        # Загружаем данные о традиционных активах
        df = yf.download(ticker, start=start_date, progress=False)
        df = df[['Close']].reset_index()  # Берем дату и цену закрытия
        df.columns = ['Date', 'Close']  # Принудительно задаем имена столбцов как строки
        df['date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')  # Преобразуем дату
        df['symbol'] = name  # Добавляем название актива
        df = df.rename(columns={'Close': 'price'})  # Переименовываем Close в price
        print(f"Columns in df for {name}: {df.columns.tolist()}")  # Отладка
        df[['date', 'symbol', 'price']].to_sql('prices', conn, if_exists='append', index=False)

# Создаем таблицу с дневной доходностью
# Создаем таблицу с дневной доходностью
cursor.execute('''
    CREATE TABLE IF NOT EXISTS returns AS
    SELECT 
        date,
        symbol,
        price,
        (price / LAG(price) OVER (PARTITION BY symbol ORDER BY date) - 1) AS daily_return
    FROM prices
''')
conn.commit()

# Проверяем и удаляем дубликаты по date и symbol
cursor.execute('DELETE FROM returns WHERE rowid NOT IN (SELECT MIN(rowid) FROM returns GROUP BY date, symbol)')
conn.commit()

# Извлекаем данные для расчета волатильности
returns_data = pd.read_sql('SELECT date, symbol, daily_return FROM returns WHERE daily_return IS NOT NULL', conn)
print(f"Returns data shape: {returns_data.shape}")  # Отладка
print(returns_data.head())  # Показываем первые строки
returns_pivot = returns_data.pivot(index='date', columns='symbol', values='daily_return')

# Считаем волатильность с помощью Python
# Извлекаем данные для расчета волатильности
returns_data = pd.read_sql('SELECT date, symbol, daily_return FROM returns WHERE daily_return IS NOT NULL', conn)
returns_pivot = returns_data.pivot(index='date', columns='symbol', values='daily_return')

# Вычисляем годовую волатильность с помощью pandas
volatility = returns_pivot.std() * (252 ** 0.5)  # Стандартное отклонение * корень из 252
volatility = volatility.reset_index().rename(columns={'index': 'symbol', 0: 'annualized_volatility'})

# Показываем таблицу волатильности
st.subheader("Волатильность")
st.write(volatility)

# Получаем данные для корреляции
returns_data = pd.read_sql('SELECT date, symbol, daily_return FROM returns WHERE daily_return IS NOT NULL', conn)
returns_pivot = returns_data.pivot(index='date', columns='symbol', values='daily_return')  # Преобразуем в таблицу

# Считаем корреляции
correlation = returns_pivot.corr()

# Показываем корреляционную матрицу
st.subheader("Корреляционная матрица")
fig = px.imshow(correlation, text_auto=True, color_continuous_scale='RdBu_r')  # Тепловая карта
st.plotly_chart(fig)

# Показываем график цен
st.subheader("Ценовые тенденции")
prices_data = pd.read_sql('SELECT date, symbol, price FROM prices', conn)
fig = go.Figure()

# Определяем цвета для каждого актива
colors = {'Bitcoin': 'blue', 'Ethereum': 'green', 'S&P 500': 'red', 'Золото': 'gold'}
for symbol in prices_data['symbol'].unique():
    df = prices_data[prices_data['symbol'] == symbol]
    fig.add_trace(go.Scatter(x=df['date'], y=df['price'], mode='lines', name=symbol, line=dict(color=colors.get(symbol, 'gray'))))

# Настраиваем макет графика
fig.update_layout(
    title="Тенденции изменения цен на активы",
    xaxis_title="Date",
    yaxis_title="Price (USD)",
    legend_title="Assets",
    xaxis=dict(rangeslider=dict(visible=True), type="date"),  # Добавляем ползунок для масштабирования
    yaxis=dict(type="log" if st.checkbox("Use Log Scale", value=False) else "linear"),  # Опционально логарифмическая шкала
    height=600,  # Увеличиваем высоту графика
    margin=dict(l=40, r=40, t=40, b=40)  # Убираем лишние отступы
)

st.plotly_chart(fig)
# Выводим ключевые выводы
st.subheader("Вывод")
st.write("""
- Волатильность активов:
Криптовалюты (Bitcoin, Ethereum) демонстрируют значительно более высокую годовую волатильность по сравнению с традиционными активами (S&P 500, Gold). Это ожидаемо, так как криптовалюты подвержены резким колебаниям спроса и настроений рынка. Например, волатильность Bitcoin может составлять 60-80% в год, в то время как S&P 500 обычно находится в диапазоне 15-20%.
Традиционные активы (S&P 500, Золото) показывают более низкую волатильность, что делает их более стабильными для долгосрочных инвестиций. Gold, как защитный актив, может иметь волатильность около 10-15%.
- Корреляция между активами:
Низкая корреляция между криптовалютами и традиционными активами (S&P 500, Золото) указывает на потенциал для диверсификации портфеля. Например, корреляция между Bitcoin и S&P 500 может быть близка к 0.2-0.4, в то время как корреляция между Gold и S&P 500 обычно отрицательная или близка к нулю.
Внутри криптовалют (Bitcoin и Ethereum) корреляция, вероятно, высока (0.7-0.9), что отражает их общую зависимость от рыночных трендов в криптоиндустрии.
- Тренды цен:
Bitcoin и Ethereum могли показать значительный рост или падение за последний год, с резкими пиками и спадами, особенно в периоды новостей (например, регуляторных изменений или хакерских атак).
S&P 500 демонстрирует более плавный рост, отражающий общую экономическую стабильность (если не было рецессии в 2025 году).
Зо мог оставаться относительно стабильным или расти в периоды нестабильности на рынке, выступая в роли "тихой гавани".
- Потенциал для инвестиций:
Диверсификация: Низкая корреляция между криптовалютами и традиционными активами (S&P 500, Gold) позволяет использовать их в портфеле для снижения рисков.
Риски: Высокая волатильность Bitcoin и Ethereum делает их рискованными для краткосрочных инвестиций, но они могут быть выгодны для долгосрочных стратегий при правильном тайминге.
Стабильность: S&P 500 и Золота подходят для более консервативных инвесторов, особенно Gold в периоды экономической неопределенности.
Дополнительные наблюдения
""""")

# Закрываем соединение с базой
conn.close()

import pandas as pd
import sqlite3

conn = sqlite3.connect('C:/Users/Fnasdmjosmofisa/Desktop/project/crypto_data.db')
prices_df = pd.read_sql('SELECT * FROM prices', conn)
returns_df = pd.read_sql('SELECT * FROM returns', conn)
prices_df.to_csv('prices.csv', index=False)
returns_df.to_csv('returns.csv', index=False)
conn.close()


