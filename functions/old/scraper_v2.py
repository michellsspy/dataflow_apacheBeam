from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import csv

# Definindo o user-agent que você deseja usar
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'

# Caminho do arquivo CSV onde os dados serão salvos
csv_file = 'data.csv'

# Abrir o arquivo CSV em modo de escrita
with open(csv_file, "w", newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    # Escrever os cabeçalhos das colunas
    writer.writerow(['Título', 'Endereço', 'Preço', 'Área', 'Quartos', 'Banheiros', 'Garagens'])

    # Iterar sobre as páginas
    for i in range(20):
        # Criando um objeto Request com o URL e o user-agent
        req = Request(f'https://www.chavesnamao.com.br/imoveis-a-venda/sc-balneario-camboriu/?pg={i}', headers={'User-Agent': user_agent})

        # Fazendo a solicitação usando urlopen com o objeto Request
        html = urlopen(req)

        # Parseie o HTML com BeautifulSoup
        bs = BeautifulSoup(html.read(), 'html.parser')

        # Encontrar os elementos desejados
        spans = bs.find_all('span', {'class': 'contentCard'})

        # Escrever os dados no arquivo CSV
        for span in spans:
            # Extrair os dados de cada elemento
            texto = ['com', 'à', 'de']
            title_element = span.find('h2')
            if title_element:
                t = title_element.text.strip()
                t_words = t.split()
                # Verificar se a segunda palavra não está na lista de palavras a serem removidas
                if t_words[1] not in texto:
                    # Se não estiver na lista, formar o título sem a segunda palavra
                    title = f'{t_words[0]} {t_words[1]}'
                else:
                    # Se estiver na lista, formar o título apenas com a primeira palavra
                    title = t_words[0]
            else:
                title = ''
            
            address_element = span.find('address')
            if address_element:
                address = address_element.text.strip()
            else:
                address = ''
            
            price_element = span.find('p', {'class': 'price'})
            if price_element:
                price = price_element.text.strip()
            else:
                price = ''
            
            # Extrair os dados de área, quartos, banheiros e garagens
            ul_element = span.find('ul', {'class': 'list im'})
            if ul_element:
                items = ul_element.find_all('li')
                # Se houver pelo menos 4 itens, extrair os dados
                if len(items) >= 4:
                    # Verificando se há o suficiente para extrair área, quartos, banheiros e garagens
                    area = items[0].text.strip().replace('m²', '')
                    quartos = items[1].text.strip()
                    banheiros = items[2].text.strip()
                    garagens = items[3].text.strip()
                else:
                    # Se não houver o suficiente, definir como vazio
                    area, quartos, banheiros, garagens = '', '', '', ''
            else:
                # Se não houver ul_element, definir como vazio
                area, quartos, banheiros, garagens = '', '', '', ''

            # Escrever os dados na linha do CSV
            writer.writerow([title, address, price, area, quartos, banheiros, garagens])
