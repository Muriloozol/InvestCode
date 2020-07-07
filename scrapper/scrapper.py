import time
import os

import pandas as pd
import requests as re
import numpy as np
import asyncio
from aiohttp import ClientSession
from loguru import logger


url_general = 'https://www.fundamentus.com.br/resultado.php'
url_paper = 'https://www.fundamentus.com.br/detalhes.php?papel='          

data_to_save = list()

def get_papers() -> pd.DataFrame:
    res = re.get(url_general)
    
    df = pd.read_html(res.content)[0]
    
    # Filter the companies without recent activity
    df = df[df['Liq.2meses'] != '000']
    
    df.set_index('Papel', inplace=True)
    
    return df[df['Cotação'] > 0]

async def get_paper_info(paper: str, save=False) -> pd.Series:
    async with ClientSession() as session:
        time.sleep(0.1)
        
        resp = await session.request(method='GET', url=url_paper+paper)
        
        time.sleep(0.1)
        
        html = await resp.text()
        df_list = pd.read_html(html)
    
    cleaner = lambda x: x.replace('?', '') if isinstance(x, str) else x
    
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
        df0[0][1:],
        df0[2],
        df1[0].append(df1[2]),
        'oscilations_' + df2[0],
        'indicator_' + df2[2].append(df2[4]),
        df3[0].append(df3[2]),
        'demonstrative12m_' + df4[0],
        'demontrative3m_' + df4[2]], ignore_index=True)

    data = df0[1][1:].append([df0[3],
                              df1[1], df1[3],
                              df2[1], df2[3], df2[5],
                              df3[1], df3[3],
                              df4[1], df4[3]]).values
    
    output = pd.Series(index=index, data=data, name=paper) 
    
    if save: data_to_save.append(output)
        
    logger.info(f'Terminou: {paper}')
    
    return output

def load_csv(path='./data/data.csv', index_col='Unnamed: 0') -> pd.DataFrame:
    return pd.read_csv(path, index_col=index_col)

async def wrapper() -> bool:
    status = False
    papers = get_papers().index
    
    # Try to load data from previous attempts
    try:
        partial = load_csv(path='partial_data.csv').index
        papers = papers.difference(partial)
        if papers.empty: 
            logger.info('You are up to date')
            return status

    except FileNotFoundError:
        logger.info('Arquivo partial_data.csv não encontrado, um novo será gerado')
    
    logger.info(f'Procurando por {papers.shape}...')
    
    tasks = list()
    for paper in papers:
        tasks.append(get_paper_info(paper, save=True))
        
    try:
    	await asyncio.gather(*tasks)
        
    # Errors collecting data
    except:
        df = pd.DataFrame(data_to_save)
        df.to_csv('partial_data.csv', mode='a')
        logger.info('Dados parciais salvos')
        status = True
        
    # Logs about every operation
    finally:
        df = pd.DataFrame(data_to_save)
        success = df.shape[0]/papers.shape[0]
        
        logger.info(f'{df.shape[0]} Salvos')
        logger.info(f'Taxa de resposta: {np.round(100*success)}%')
        
        return status
        
def run_wrapper():
    status = True
    while status:
        data_to_save = list()
        status = asyncio.run(wrapper())
        
        if status: 
            logger.info('Refazendo operação')
            time.sleep(5)
            
        # All papers reached
        else: 
            df = pd.DataFrame(data_to_save)
            df.to_csv('partial_data.csv', mode='a')

            today = pd.Timestamp.today()
            day, month, year = today.day, today.month, today.year
            
            # Save data_day_month_year.csv at data directory remove 
            final_data = load_csv('partial_data.csv')
            final_data = final_data[final_data.index.notna()]
            final_data.to_csv(f'../data/data_{day}_{month}_{year}.csv')

            # Remove partial result
            os.remove('partial_data.csv')

            logger.info('DADOS SALVOS NA PASTA DATA')
        
        
if __name__ == '__main__':
    run_wrapper()
