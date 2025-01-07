##Usando Apache Airflow e Web Scraping para Explorar Dados do FBref ⚽

🎯 Explorando Dados de Futebol com Airflow e FBref ⚽
Nos meus estudos recentes sobre ferramentas de dados, desenvolvi uma DAG no Apache Airflow para realizar web scraping do site FBref, uma das melhores fontes para dados atualizados sobre ligas, clubes e jogadores de futebol. Este projeto destacou a importância de ferramentas modernas para automação e coleta de dados no universo do futebol. O projeto é uma continuação de um outro projeto meu. 

⚙️ Airflow: Automação inteligente no pipeline de dados
O Apache Airflow é uma ferramenta poderosa para a orquestração de workflows. Ele facilita a execução de tarefas complexas e garante que processos, como a coleta de dados do FBref, sejam automatizados e escaláveis.

💡 Por que dados atualizados são cruciais no scouting?
Para analistas e profissionais de scouting, trabalhar com informações recentes permite identificar tendências, avaliar jogadores em ascensão e comparar performances de maneira eficiente. Dados atualizados podem fazer a diferença entre descobrir um talento antes da concorrência ou perder oportunidades valiosas.

🔑 Destaques da minha implementação no Airflow
1️⃣ Validação de arquivos locais: Antes de realizar novas coletas, a DAG verifica se os dados já existem e estão atualizados. Isso reduz processamento desnecessário.
2️⃣ Web scraping estruturado: Utilizei bibliotecas como BeautifulSoup para navegar no site e capturar informações detalhadas de times e competições.
3️⃣ Transformação de dados: As tabelas são processadas e integradas, permitindo uma visão consolidada de estatísticas.
4️⃣ Escalabilidade e automação: Com operadores Python, a DAG está configurada para rodar periodicamente e gerenciar falhas com eficiência.
5️⃣ Armazenamento final dos dados estruturados no Google Cloud Storage, garantindo segurança, escalabilidade e armazenamento dos dados históricos.
