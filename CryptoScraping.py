# Description: Coinmarketcap web scraping project
# Created by: Lyubomir Lirkov
# Create on: 08/09/2021

'''
This project is about scraping crypto data from coinmarketcap. It extracts the data and then transforms it, so it can be stored in a dictionary. 
DataFreme is created at the end, in order to represent the information stored inside the dictionary.
'''

# imports
import requests
from bs4 import BeautifulSoup 
import pandas as pd

def extract(page):
    
    # user agent helps avoiding blocking by automated scraping systems
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}
    
    # webpage url
    url = f'https://coinmarketcap.com/currencies/{page}/'
    
    # querying the server to get data back
    r = requests.get(url, headers)
    
    # soup variable containing text of the url request
    soup = BeautifulSoup(r.content, 'html.parser')
    
    return soup

content = extract('bitcoin')


def transform(soup):
    
    divs = soup.find_all('div', class_ = 'sc-16r8icm-0 nds9rn-0 dAxhCK')
    
    # store each <td>'s data
    td_list = []
    
    for each in divs:
        for index in range(9):
            title = each.find_all('td')[index].text
            td_list.append(title)
            #print(title)
    
    # store each <span>'s data
    span_list = []
    
    for each in divs:
        for index in range(9):
            span = each.find_all('span')[index].text
            span_list.append(span)
    
    # data transformations 
    # price
    price = [each for each in td_list[0] if each.isdigit() == True or each in '.']
    price = float(''.join(price))
    
    # 24h price change
    price_change_24h = [each for each in span_list[2] if each.isdigit() == True or each in '-.']
    price_change_24h = float(''.join(price_change_24h))
    
    # 24h low / high
    low_high_24h = [each for each in td_list[2] if each.isdigit() == True or each in ('/.')]
    low_high_24h = ''.join(low_high_24h)
    low_high_split = low_high_24h.split('/')
    low = float(low_high_split[0])
    high = float(low_high_split[1])
    
    # 24h trading volume
    trading_volume_24h = [each for each in span_list[7] if each.isdigit() == True or each in '.']
    trading_volume_24h = float(''.join(trading_volume_24h))
    
    # 24h trading volume change
    
    # volume / market cap
    volume_marketcap = float(td_list[4])
    
    # market dominance %
    market_dominance = float(td_list[5].rstrip('%'))
    
    # market rank
    market_rank = int(td_list[6].lstrip('#'))
    
    # dictionary to store the data
    crypto_dictionary = {
        'Price': price,
        'Price Change 24h': price_change_24h,
        '24h Low': low,
        '24h High': high,
        'Trading Volume 24h': trading_volume_24h,
        'Volume / Market Cap': volume_marketcap,
        'Market Dominance (%)': market_dominance,
        'Market Rank': market_rank
    }
    
    # using the dictionary to create a DataFrame
    crypto_table = pd.DataFrame.from_dict([crypto_dictionary])
    
    return crypto_table


display(transform(content))


