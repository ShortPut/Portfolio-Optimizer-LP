import config
import psycopg2
import psycopg2.extras
import yfinance as yf
import numpy as np
import pandas as pd
import pulp

CAPITAL = 1000000

# Connect to sp500_data database.
connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)

cursor =  connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

def get_indexes(specific_tickers):
    indexes = []
    for j in range(len(specific_tickers)):
        for i in range(len(tickers)):
            if specific_tickers[j] == tickers[i]:
                indexes.append(i)
                break
    return indexes

# Create lists with relevant prices (coefficients) and tickers (variables) needed for objective function and constraints.

### List for Objective function: total invested
cursor.execute("SELECT * FROM sp500_table ORDER BY ticker ASC")
connection.commit()
table_val = cursor.fetchall()
tickers = []
prices = []
for row in table_val:
    tickers.append(row[0])
    prices.append(row[2])
obj_tickers_vars = [pulp.LpVariable("{}".format(ticker)) for ticker in tickers]

total_invest = sum(price * ticker for price,ticker in zip(prices,obj_tickers_vars))


### Lists for each sector
##### Basic Materials
cursor.execute("SELECT * FROM sp500_table WHERE sector='Basic Materials' ORDER BY ticker ASC")
connection.commit()
basic_material_table = cursor.fetchall()
basic_material_tickers = []
basic_material_prices = []
for row in basic_material_table:
    basic_material_tickers.append(row[0])
    basic_material_prices.append(row[2])
basic_material_indexes = get_indexes(basic_material_tickers)
basic_material_vars = []
for i in basic_material_indexes:
    basic_material_vars.append(obj_tickers_vars[i])

total_basic_material = sum(price * ticker for price, ticker in zip(basic_material_prices,basic_material_vars))

##### Communication Services
cursor.execute("SELECT * FROM sp500_table WHERE sector='Communication Services' ORDER BY ticker ASC")
connection.commit()
communication_table = cursor.fetchall()
communication_tickers = []
communication_prices = []
for row in communication_table:
    communication_tickers.append(row[0])
    communication_prices.append(row[2])
communication_indexes = get_indexes(communication_tickers)
communication_vars = []
for i in communication_indexes:
    communication_vars.append(obj_tickers_vars[i])

total_communication = sum(price * ticker for price, ticker in zip(communication_prices,communication_vars))

##### Consumer Cyclical 
cursor.execute("SELECT * FROM sp500_table WHERE sector='Consumer Cyclical' ORDER BY ticker ASC")
connection.commit()
consumer_cyc_table = cursor.fetchall()
consumer_cyc_tickers = []
consumer_cyc_prices = []
for row in consumer_cyc_table:
    consumer_cyc_tickers.append(row[0])
    consumer_cyc_prices.append(row[2])
consumer_cyc_indexes = get_indexes(consumer_cyc_tickers)
consumer_cyc_vars = []
for i in consumer_cyc_indexes:
    consumer_cyc_vars.append(obj_tickers_vars[i])

total_consumer_cyc = sum(price * ticker for price, ticker in zip(consumer_cyc_prices,consumer_cyc_vars))

##### Consumer Defensive
cursor.execute("SELECT * FROM sp500_table WHERE sector='Consumer Defensive' ORDER BY ticker ASC")
connection.commit()
consumer_def_table = cursor.fetchall()
consumer_def_tickers = []
consumer_def_prices = []
for row in consumer_def_table:
    consumer_def_tickers.append(row[0])
    consumer_def_prices.append(row[2])
consumer_def_indexes = get_indexes(consumer_def_tickers)
consumer_def_vars = []
for i in consumer_def_indexes:
    consumer_def_vars.append(obj_tickers_vars[i])

total_consumer_def = sum(price * ticker for price, ticker in zip(consumer_def_prices,consumer_def_vars))

##### Energy
cursor.execute("SELECT * FROM sp500_table WHERE sector='Energy' ORDER BY ticker ASC")
connection.commit()
energy_table = cursor.fetchall()
energy_tickers = []
energy_prices = []
for row in energy_table:
    energy_tickers.append(row[0])
    energy_prices.append(row[2])
energy_indexes = get_indexes(energy_tickers)
energy_vars = []
for i in energy_indexes:
    energy_vars.append(obj_tickers_vars[i])

total_energy = sum(price * ticker for price, ticker in zip(energy_prices,energy_vars))

##### Financials
cursor.execute("SELECT * FROM sp500_table WHERE sector='Financial Services' ORDER BY ticker ASC")
connection.commit()
financials_table = cursor.fetchall()
financials_tickers = []
financials_prices = []
for row in financials_table:
    financials_tickers.append(row[0])
    financials_prices.append(row[2])
financials_indexes = get_indexes(financials_tickers)
financials_vars = []
for i in financials_indexes:
    financials_vars.append(obj_tickers_vars[i])

total_financials = sum(price * ticker for price, ticker in zip(financials_prices,financials_vars))

##### Healthcare
cursor.execute("SELECT * FROM sp500_table WHERE sector='Healthcare' ORDER BY ticker ASC")
connection.commit()
healthcare_table = cursor.fetchall()
healthcare_tickers = []
healthcare_prices = []
for row in healthcare_table:
    healthcare_tickers.append(row[0])
    healthcare_prices.append(row[2])
healthcare_indexes = get_indexes(healthcare_tickers)
healthcare_vars = []
for i in healthcare_indexes:
    healthcare_vars.append(obj_tickers_vars[i])

total_healthcare = sum(price * ticker for price, ticker in zip(healthcare_prices,healthcare_vars))

##### Industrials
cursor.execute("SELECT * FROM sp500_table WHERE sector='Industrials' ORDER BY ticker ASC")
connection.commit()
industrials_table = cursor.fetchall()
industrials_tickers = []
industrials_prices = []
for row in industrials_table:
    industrials_tickers.append(row[0])
    industrials_prices.append(row[2])
industrials_indexes = get_indexes(industrials_tickers)
industrials_vars = []
for i in industrials_indexes:
    industrials_vars.append(obj_tickers_vars[i])

total_industrials = sum(price * ticker for price, ticker in zip(industrials_prices,industrials_vars))

##### Real Estate
cursor.execute("SELECT * FROM sp500_table WHERE sector='Real Estate' ORDER BY ticker ASC")
connection.commit()
real_estate_table = cursor.fetchall()
real_estate_tickers = []
real_estate_prices = []
for row in real_estate_table:
    real_estate_tickers.append(row[0])
    real_estate_prices.append(row[2])
real_estate_indexes = get_indexes(real_estate_tickers)
real_estate_vars = []
for i in real_estate_indexes:
    real_estate_vars.append(obj_tickers_vars[i])

total_real_estate = sum(price * ticker for price, ticker in zip(real_estate_prices,real_estate_vars))

##### Technology
cursor.execute("SELECT * FROM sp500_table WHERE sector='Technology' ORDER BY ticker ASC")
connection.commit()
tech_table = cursor.fetchall()
tech_tickers = []
tech_prices = []
for row in tech_table:
    tech_tickers.append(row[0])
    tech_prices.append(row[2])
tech_indexes = get_indexes(tech_tickers)
tech_vars = []
for i in tech_indexes:
    tech_vars.append(obj_tickers_vars[i])

total_tech = sum(price * ticker for price, ticker in zip(tech_prices,tech_vars))

##### Utilities
cursor.execute("SELECT * FROM sp500_table WHERE sector='Utilities' ORDER BY ticker ASC")
connection.commit()
utilities_table = cursor.fetchall()
utilities_tickers = []
utilities_prices = []
for row in utilities_table:
    utilities_tickers.append(row[0])
    utilities_prices.append(row[2])
utilities_indexes = get_indexes(utilities_tickers)
utilities_vars = []
for i in utilities_indexes:
    utilities_vars.append(obj_tickers_vars[i])

total_utilities = sum(price * ticker for price, ticker in zip(utilities_prices,utilities_vars))

### List of stocks with Beta > 1.5
cursor.execute("SELECT * FROM sp500_table WHERE beta>1.5 ORDER BY ticker ASC")
connection.commit()
high_beta_table = cursor.fetchall()
high_beta_tickers = []
high_beta_prices = []
for row in high_beta_table:
    high_beta_tickers.append(row[0])
    high_beta_prices.append(row[2])
high_beta_indexes = get_indexes(high_beta_tickers)
high_beta_vars = []
for i in high_beta_indexes:
    high_beta_vars.append(obj_tickers_vars[i])

total_high_beta = sum(price * ticker for price, ticker in zip(high_beta_prices,high_beta_vars))

### List of overvalued, high PE (>=30) stocks
cursor.execute("SELECT * FROM sp500_table WHERE pe>=30 ORDER BY ticker ASC")
connection.commit()
high_pe_table = cursor.fetchall()
high_pe_tickers = []
high_pe_prices = []
for row in high_pe_table:
    high_pe_tickers.append(row[0])
    high_pe_prices.append(row[2])
high_pe_indexes = get_indexes(high_pe_tickers)
high_pe_vars = []
for i in high_pe_indexes:
    high_pe_vars.append(obj_tickers_vars[i])

total_high_pe = sum(price * ticker for price, ticker in zip(high_pe_prices,high_pe_vars))

### List of dividend paying stocks
cursor.execute("SELECT * FROM sp500_table WHERE dividend>0 ORDER BY ticker ASC")
connection.commit()
dividend_table = cursor.fetchall()
dividend_tickers = []
dividend_prices = []
for row in dividend_table:
    dividend_tickers.append(row[0])
    dividend_prices.append(row[2])
dividend_indexes = get_indexes(dividend_tickers)
dividend_vars = []
for i in dividend_indexes:
    dividend_vars.append(obj_tickers_vars[i])

total_dividend = sum(price * ticker for price, ticker in zip(dividend_prices,dividend_vars))

# Implement objective and constraints into LP problem
Lp_prob = pulp.LpProblem('sp500_portfolio_optimizer', pulp.LpMaximize)

### Objective function
Lp_prob += total_invest

### Constraints
Lp_prob += total_invest <= CAPITAL

Lp_prob += total_basic_material <= (0.25*CAPITAL)
Lp_prob += total_communication <= (0.25*CAPITAL)
Lp_prob += total_consumer_cyc <= (0.25*CAPITAL)
Lp_prob += total_consumer_def <= (0.25*CAPITAL)
Lp_prob += total_energy <= (0.25*CAPITAL)
Lp_prob += total_financials <= (0.25*CAPITAL)
Lp_prob += total_healthcare <= (0.25*CAPITAL)
Lp_prob += total_industrials <= (0.25*CAPITAL)
Lp_prob += total_real_estate <= (0.25*CAPITAL)
Lp_prob += total_tech <= (0.25*CAPITAL)
Lp_prob += total_utilities <= (0.25*CAPITAL)

Lp_prob += total_high_beta <= (0.2*CAPITAL)

Lp_prob += total_high_pe <= (0.25*CAPITAL)

Lp_prob += total_dividend >= (0.5*CAPITAL)

for i in range(0, len(tickers)):
    Lp_prob += prices[i] * obj_tickers_vars[i] <= (0.05*CAPITAL)
    Lp_prob += obj_tickers_vars[i] >= 0

Lp_prob.solve()

# Print optimal solution
# for variable in Lp_prob.variables():
#     print(variable.name, "=", variable.varValue)
# print("Optimal value is Total Maximum Investment = ", pulp.value(Lp_prob.objective))

# Print optimal solution excluding 0 values (stocks we don't buy)
# for variable in Lp_prob.variables():
#    if variable.varValue > 0: 
#         print(variable.name, "=", variable.varValue)
# print("Optimal value is Total Maximum Investment = ", pulp.value(Lp_prob.objective))

# Optimal solution without fractional shares
for variable in Lp_prob.variables():
   if variable.varValue > 0: 
        print(variable.name, "=", np.floor(variable.varValue))
print("Optimal value is Total Maximum Investment = ", np.floor(pulp.value(Lp_prob.objective)))

# print("Status: " + pulp.LpStatus[Lp_prob.status])