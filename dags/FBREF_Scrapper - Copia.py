from airflow import DAG
from datetime import datetime,timedelta
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from airflow.models import Variable #importa variaveis do airflow
import warnings
import pandas as pd
from bs4 import BeautifulSoup
import requests
from helper import Helper
import json
import re
import time
import os
from tenacity import retry, stop_after_attempt, wait_exponential


# Ignorar FutureWarnings
warnings.filterwarnings('ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None





#2 - Configurações da DAG

default_args = {
    'depends_on_past' :False,
    'email': ['dpsoeiro@hotmail.com'] ,
    'email_on_failure' : True, #Envio de email em caso de falha
    'email_on_retry' : False,
    'retries':1,
    'retry_delay': timedelta(seconds = 10)}

dag = DAG('import_fbref_2',
          description ='Traz dados do FBREF',
          schedule_interval = None,
          default_args = default_args,
          default_view = 'graph',
          start_date = datetime(2024,12,9),
          dagrun_timeout=timedelta(hours=6), 
          catchup = False)


# 3- Funções

# 3.1 - Funçoes que pega as competições disponiveis



def file_available(file_path, **kwargs):
    if os.path.exists(file_path):
        timestamp_modificacao = os.path.getmtime(file_path)
        data_modificacao = datetime.fromtimestamp(timestamp_modificacao)

        data_atual = datetime.now()
        seis_meses_atras = data_atual - timedelta(days=6*30)

        if data_modificacao > seis_meses_atras:
            return True #arquivo existe, foi atualizado nos ultimos 6 meses 
        else:
            return False #arquivo existe, mas não foi atualizado nos ultimos 6 meses 
    else:
        return False #arquivo não existe


def open_json(file_name,**kwargs):
    path = file_name
    with open(path, 'r', encoding='utf-8') as file:
        open_data = json.load(file)
    return open_data




def get_competition_coverage_all(**kwargs):

    status_file_available = file_available(Variable.get('path_file_competitions_FBREF'))
    
    if status_file_available == True :
        competitions = open_json(Variable.get('path_file_competitions_FBREF'))
  
        kwargs['ti'].xcom_push(key='dict_all_competitions', value=competitions)
        kwargs['ti'].xcom_push(key='status_all_competitions', value='arquivo_existente')
    else:

        header = Helper.get_user_agent()
        data = requests.get('https://fbref.com/en/about/coverage',headers=header)
        soup = BeautifulSoup(data.text, features="lxml")

        competitions = []
        for class_tier in soup.find_all('th', {'data-stat': 'Tier'}):

            comp_sibling = class_tier.find_next_sibling('td', {'data-stat': 'comp'})

            if comp_sibling is not None:
  
                comp_infos = class_tier.find_next_sibling('td', {'data-stat': 'comp'})
                comp_href = comp_infos.find('a').get('href')

                nome_competicao = comp_sibling.text.strip()
                competition_data = {
                            "comp": nome_competicao,
                            "tier": class_tier.text.strip(),
                            "country": class_tier.find_next_sibling('td', {'data-stat': 'country'}).text.strip(),
                            "hist_link": r'https://fbref.com' + comp_href,
                            "stats_link":r'https://fbref.com' + comp_href.replace('Seasons','Stats').replace('/history/','/'),
                            "gender": class_tier.find_next_sibling('td', {'data-stat': 'gender'}).text.strip(),
                            "seasons": class_tier.find_next_sibling('td', {'data-stat': 'Seasons'}).text.strip(),
                            "minseason": class_tier.find_next_sibling('td', {'data-stat': 'minseason'}).text.strip(),
                            "maxseason": class_tier.find_next_sibling('td', {'data-stat': 'maxseason'}).text.strip()
                            }

                competitions.append(competition_data)

        with open(Variable.get('path_file_competitions_FBREF'), 'w') as json_file:
            json.dump(competitions, json_file)

        
        kwargs['ti'].xcom_push(key='dict_all_competitions', value=competitions)
        kwargs['ti'].xcom_push(key='status_all_competitions', value='arquivo_atualizado')


#@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
def return_team_links(**kwargs):


    status_file_available = file_available(Variable.get('file_path_teams_available'))



    if status_file_available == True:
        teams = open_json(Variable.get('file_path_teams_available'))
  
        kwargs['ti'].xcom_push(key='dict_all_teams', value=teams)
        kwargs['ti'].xcom_push(key='status_all_teams', value='arquivo_existente')

    else:

        dict_competitions = kwargs['ti'].xcom_pull(key='dict_all_competitions',task_ids ='get_data_all_competitions')
        teams_available = {}

        for i in range(len(dict_competitions)):

           # time.sleep(2)

            competition = dict_competitions[i]

            if competition["country"] != "" and competition["gender"] == "M" and "2024" in competition["maxseason"] and competition["tier"] != '':
                link = competition["stats_link"]
                comp_name = competition["comp"]
            
                header = Helper.get_user_agent()
                data = requests.get(link,headers=header)
                soup = BeautifulSoup(data.text, features="lxml")

                if data.status_code != 200:
                    for attempt in range(3):  # Tenta no máximo 3 vezes
                        time.sleep(10)
                        header = Helper.get_user_agent()
                        data = requests.get(link, headers=header)
                        if data.status_code == 200:
                            soup = BeautifulSoup(data.text, features="lxml")
                            break  # Sai do loop se o código de status for 200
                    else:
                        teams_available[comp_name] = 'Erro'
                        continue

                list_links_competition = set()

                for team in soup.find_all('th', {'data-stat': 'rank'}):
    
                    team_rank = team.find_next_sibling('td', {'data-stat': 'team'})
    
                
                    if team_rank is not None:
                        team_link = team_rank.find('a')
                        team_link = team_link.get('href') if team_link else ""
                        team_link = 'https://fbref.com{}'.format(team_link)
                        list_links_competition.add(team_link)


                teams_available[comp_name] = list(list_links_competition)

        with open(Variable.get('file_path_teams_available'), 'w') as json_file:
            json.dump(teams_available, json_file)

        kwargs['ti'].xcom_push(key='dict_all_teams', value=teams_available)


def return_team_information(**kwargs):

    dict_teams_links = kwargs['ti'].xcom_pull(key='dict_all_teams',task_ids ='return_all_team_links')
    dataframes = []

    for competition,list_links in dict_teams_links.items():

        list_links.remove("https://fbref.com") if "https://fbref.com" in list_links else list_links

        for link in list_links:

            time.sleep(5)
            header = Helper.get_user_agent()
            data = requests.get(link,headers= header)
            soup = BeautifulSoup(data.text, features="lxml")
            tables = soup.find_all("table")


            match = re.search(r'/([^/]+)-Stats$', link)
            ext_texto = match.group(1)
            club_name = ext_texto.replace('-',' ')
            
            for i in range(0,len(tables)-2):

                # Encontrar o título mais próximo
                table = tables[i]
                title = table.find_previous("h2") or table.find_previous("h3") or table.find_previous("h1")
                title_text = title.get_text(strip=True) if title else "Sem título"
            
                # Usar pd.read_html para ler a tabela
            
                if 'Goalkeeping' in title_text or 'Advanced Goalkeeping' in title_text or 'Scores & Fixtures' in title_text or "Sem título" in title_text:
                    continue       
                elif 'Standard Stats' in title_text:
                    df_stats = pd.read_html(str(table))[0]
                    df_stats.columns = Helper.adjust_column(df_stats)
                    df_stats = df_stats.drop_duplicates().fillna(0)
                    df_stats['Competition'] = competition
                    df_stats['Club'] = club_name
                    
                else:
                    df_stats_new = pd.read_html(str(table))[0]
                    df_stats_new.columns = Helper.adjust_column(df_stats_new)

                    if 'Player' not in list(df_stats_new.columns):
                        continue
                    else:      
                        column_final = [item for item in list(df_stats_new.columns) if item not in list(df_stats.columns)]
                        column_final.extend(['Player'])
                        df_stats_new = df_stats_new.loc[:, df_stats_new.columns.isin(column_final)]
                        df_stats_new = df_stats_new.drop_duplicates().fillna(0)
                
                        df_stats = pd.merge(df_stats, df_stats_new, how='left', on='Player')
                        
            
            dataframes.append(df_stats)

    df_stats = pd.concat(dataframes, ignore_index=True).fillna(0)
    df_stats =  df_stats [(df_stats['Player'] != 'Squad Total') & (df_stats['Player'] != 'Opponent Total')].reset_index().drop(columns = 'index')
    df_stats.to_csv(Variable.get('path_stats_teams'),sep = ';',encoding= 'utf-8')




#4 - Criação dos Operadores

get_data_all_competitions  = PythonOperator(task_id = 'get_data_all_competitions',
                                            python_callable = get_competition_coverage_all,
                                            provide_context = True,
                                            dag=dag)

return_all_team_links  = PythonOperator(task_id = 'return_all_team_links',
                                            python_callable = return_team_links,
                                            provide_context = True,
                                            execution_timeout=timedelta(hours=2),
                                            dag=dag)

return_all_team_stats = PythonOperator(task_id = 'return_all_team_stats',
                                            python_callable = return_team_information,
                                            provide_context = True,
                                            execution_timeout=timedelta(hours=5),
                                            dag=dag)

#5 - Ordem das DAGs

get_data_all_competitions >> return_all_team_links >> return_all_team_stats

            

          






