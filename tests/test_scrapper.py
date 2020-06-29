import pandas as pd
import pytest

from scrapper import scrapper as scr

def test_get_papers():
    data = scr.get_papers()
    
    assert isinstance(data, pd.DataFrame)
    assert all(data['Liq.2meses'] != '000')
    assert data.index.name == 'Papel'
    
@pytest.mark.parametrize('paper', ['bidi4', 'btow3', 'idvl3', 'sanb11'])
def test_get_paper_info(paper):
    data = scr.get_paper_info(paper)
    
    assert isinstance(data, pd.Series)
    assert data.name == paper
    assert data.shape != (0,)

@pytest.mark.skip
def test_save_data():
    pass

@pytest.mark.skip
def test_load_data():
    pass