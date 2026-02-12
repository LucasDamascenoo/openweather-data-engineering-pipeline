import requests
import json
from pathlib import Path
import logging



#Configuracao do Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

def extract_weather_data (url:str) ->list:
    
    #chamando a api
    response = requests.get(url)    
    data = response.json() 
    
    
    if response.status_code != 200:
        logging.error('Erro na requisicao')
        return []      
        
    
    if not data:
        logging.warning('nenhum dado retornado')
        return []
        
    
    #onde queremos salvar os dados
    output_path = Path('data/weather_data.json')
    #para criar essa pasta>caso nao exista
    output_path.parent.mkdir(parents=True,exist_ok=True)    
    
    #escreve o arquivo na pasta correta (sem isso o arquivo nao existe/nao add)
    with output_path.open('w') as f:
        json.dump(data,f)
        
        
    
    logging.info(f'O Arquivo foi salvo em {output_path}')
    
    return data
    
