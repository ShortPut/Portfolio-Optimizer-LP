import config
import psycopg2
import psycopg2.extras
import yfinance as yf
import numpy as np
import pandas as pd

# Connect to sp500_data database.
connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)

cursor =  connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Get the data from sp500_table and extract ticker col into str list
cursor.execute("SELECT ticker FROM sp500_table ORDER BY ticker ASC")
connection.commit()
table_val = cursor.fetchall()
tickers = []
for row in table_val:
    tickers.append(row[0])

# Get current sector for each ticker and place into sp500_table
for ticker in tickers:
    try:
        sector = yf.Ticker(ticker).info['sector']
        print(ticker,sector)
        cursor.execute(
            "UPDATE sp500_table SET sector=%s WHERE ticker=%s", (sector, ticker)
        )
        connection.commit()

    except Exception as e:
        print(ticker, e)
        connection.rollback()

