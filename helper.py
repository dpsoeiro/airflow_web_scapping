import json
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class Helper(BaseOperator):

    @apply_defaults

    def __init__(self):
        self.file_path = 'C:\\Users\\dpsoe\\OneDrive\\football_web_scrapper\\'
    


    def get_user_agent():


        software_names = [SoftwareName.CHROME.value, SoftwareName.FIREFOX.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
        user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems)
        random_user_agent = user_agent_rotator.get_random_user_agent()
        header = {'User-Agent':str(random_user_agent)}

        return header

    def extract_number(text):
        # Divide a string pelos caracteres de barra ("/")
        text_parts = text.split('/')

        # Procura o número na lista resultante
        for part in text_parts:
            if part.isdigit():
                return part
            
    def adjust_column(self,df):
        df_columns = [' '.join(col).strip() for col in df.columns]
        new_columns = []

        for col in df_columns:
            if 'level_0' in col:
                new_col = col.split()[-1]  # takes the last name
            else:
                new_col = col
            new_columns.append(new_col)
        return new_columns     
    

    def file_availabe(self,file_name):
        if os.path.exists(file_name):
            return True
        else:
            return False
        

    def open_json(self,file_name):
    #    path = self.file_path + file_name
        path = file_name
        with open(path, 'r', encoding='utf-8') as file:
            open_data = json.load(file)

        return open_data