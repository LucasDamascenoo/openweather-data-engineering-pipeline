from sqlalchemy import create_engine,text
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pathlib import Path
import os
import pandas as pd
import logging


#Configuracao do Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


env_path = Path(__file__).resolve().parent.parent / 'config'/ '.env'
load_dotenv(env_path)

user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database')
host = 'localhost' #o docker acessa o nosso bd localmente


def get_engine():
    logging.info(f"-> conectando em {host}:5432/{database}")
    return create_engine(
        
        f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:5432/{database}"
        
    )



engine = get_engine()


def load_data(table_name:str,df):
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists ='append',
        index = False
    )
    
    logging.info(f"Dados carregados com sucesso!\n")



    df_check = pd.read_sql(f"select * from {table_name}", con=engine)
    logging.info(f"total de registros na tabela: {len(df_check)}")
    

