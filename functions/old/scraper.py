from urllib.request import urlopen, Request
from bs4 import BeautifulSoup

# Definindo o user-agent que você deseja usar
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'

# Caminho do arquivo de texto onde os dados serão salvos
path_file = 'data.txt'

# Abrir o arquivo em modo de escrita
with open(path_file, "w", encoding='utf-8') as file:
    # Iterar sobre as páginas
    for i in range(10):
        # Criando um objeto Request com o URL e o user-agent
        req = Request(f'https://www.chavesnamao.com.br/imoveis-a-venda/sc-balneario-camboriu/?pg={i}', headers={'User-Agent': user_agent})

        # Fazendo a solicitação usando urlopen com o objeto Request
        html = urlopen(req)

        # Parseie o HTML com BeautifulSoup
        bs = BeautifulSoup(html.read(), 'html.parser')

        # Encontrar os elementos desejados
        spans = bs.find_all('span', {'class': 'contentCard'})

        # Escrever os dados no arquivo de texto
        for span in spans:
            file.write(str(span) + "\n")  # Escrever o texto do span seguido de uma quebra de linha
