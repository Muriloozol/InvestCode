import requests as re
import pandas as pd
import numpy as np

url_general = 'https://www.fundamentus.com.br/resultado.php'
url_paper = 'https://www.fundamentus.com.br/detalhes.php?papel='          

def get_papers():
    res = re.get(url_general)
    
    df = pd.read_html(res.content)[0]
    
    # Filter the companies without recent activity
    df = df[df['Liq.2meses'] != '000']
    
    df.set_index('Papel', inplace=True)
    
    return df[df['Cotação'] > 0]

def get_paper_info(paper: str):
    res = re.get(url_paper + paper)
        
    df_list = pd.read_html(res.text)
    
    cleaner = lambda x: x.replace('?', '')\
                         .replace('.', '')\
                         .replace(',', '.')\
                         if isinstance(x, str) else x
    
    # Separate and clean diferent tables found on page
    df0 = df_list[0].applymap(cleaner)
    df1 = df_list[1].applymap(cleaner)
    df2 = df_list[2].applymap(cleaner)
    df3 = df_list[3].applymap(cleaner)
    df4 = df_list[4].applymap(cleaner)

    # Drop some headers
    df2.drop(0, inplace=True)
    df3.drop(0, inplace=True)
    df4.drop([0, 1], inplace=True)

    index = pd.concat([
        'Info: ' + df0[0][1:],
        'PriceInfo: ' + df0[2],
        'MarketValue: ' + df1[0].append(df1[2]),
        'Oscilations: ' + df2[0],
        'FundamentalIndicators: ' + df2[2].append(df2[4]),
        'BalanceSheet: ' + df3[0].append(df3[2]),
        'Demonstrative12M: ' + df4[0],
        'Demontrative3M: ' + df4[2]], ignore_index=True)

    data = df0[1][1:].append([df0[3],
                              df1[1], df1[3],
                              df2[1], df2[3], df2[5],
                              df3[1], df3[3],
                              df4[1], df4[3]]).values

    return pd.Series(index=index, data=data, name=paper)

def save_data(papers: list):
    holder = list()
    for paper in papers.index:
        paper_info = get_paper_info(paper)
            
        holder.append(paper_info)
        
    save = pd.DataFrame(holder)
    
    save.to_csv('./data/data.csv')
    
    return save

def load_csv(path='./data/data.csv', index_col='Unnamed: 0'):
    data = pd.read_csv(path, index_col=index_col)
    
    return data
