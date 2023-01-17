import config
import psycopg2
import psycopg2.extras
import numpy as np
import pandas as pd
import wikipedia as wp

# Connect to sp500_data database.
connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)

cursor =  connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Get the ticker symbols from Wikipedia table
html = wp.page("S&P 500 component stocks").html().encode("UTF-8")
df = pd.read_html(html)[0]
tickers_df = df.iloc[: , 0]

tickers = []
for i in range(0,np.size(tickers_df)):
    tickers.append(tickers_df[i])

# Place tickers into SQL sp500_table table under ticker column
for i in tickers:
    try:
        cursor.execute(
                "INSERT INTO sp500_table (ticker) VALUES (%s)", (i,),
            )
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

