# Description: Yahoo Finance web scraping project
# Created by: Lyubomir Lirkov
# Create on: 03/09/2021

'''
This project is about scraping financial data from yahoo finance. It uses a list of favourite stocks and scrapes
their prices as well as the change in the price. This information is retrieved every ten minutes and it stored in a database.
The process is acitve only within the working hours of the NYC stock exchange. 
'''

# imports
import requests
from bs4 import BeautifulSoup 
import pyodbc
import datetime
import time

# setup database connection
conn = pyodbc.connect('Driver={SQL Server};'
                     'Server=ADATIS-LAP-BG06;'
                      'Database=Library;'
                      'Trusted_Connection=yes;'
                     )

cursor = conn.cursor()


def stockPrice(symbol):
    # user agent
    headers = {'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}

    # webpage url
    url = f'https://finance.yahoo.com/quote/{symbol}'

    # querying the server to get data back
    r = requests.get(url)

    # soup variable containing text of the url request
    soup = BeautifulSoup(r.text, 'html.parser')

    # returns the price
    price = soup.find('div', {'class':'D(ib) Mend(20px)'}).find_all('span')[0].text

    # returns the change in price
    change = soup.find('div', {'class':'D(ib) Mend(20px)'}).find_all('span')[1].text
    
    cursor.execute('''
                INSERT INTO StockMarket (Symbol, Price, Change, Date)
                VALUES (?, ?, ?, ?)
                ''',
                (symbol, price, change, datetime.datetime.today())
                )
    
    conn.commit()
        
    return price, change
    
    
# list of favourite stocks to watch for     
favourite_stocks = ['AAPL', 'NVDA', 'MSFT', 'TSLA']

# opening time of NYC stock exchange, converted to UTC +2 (Sofia, Bulgaria)
opening_time = datetime.time(16, 0, 0)
# closing time of NYC stock exchange, converted to UTC +2 (Sofia, Bulgaria)
closing_time = datetime.time(23, 0, 0)

time_now = datetime.datetime.now()
week_day = time_now.weekday()

while int(time_now.hour) >= int(opening_time.hour) and int(time_now.hour) <= int(closing_time.hour) \
        and week_day >= 0 and week_day <= 5:
    for each in favourite_stocks:
        stockPrice(each)
    time.sleep(600)

    