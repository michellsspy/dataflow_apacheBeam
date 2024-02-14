import apache_beam as beam
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import csv
import re
import numpy as np
import json

class IngestDoFn(beam.DoFn):
    # Definindo o user-agent que você deseja usar
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'

    def process(self, element):
        # Função para separar o endereço em nome da rua, número, bairro e cidade
        def separar_endereco(endereco):
            endereco = endereco.split(',')
            rua = endereco[0].strip()
            numero = None
            bairro = None
            cidade = endereco[-1].strip()
            if len(endereco) > 1:
                for item in endereco[1:-1]:
                    item = item.strip()
                    if re.match(r'^\d+$', item):  # Verifica se o item é um número
                        numero = item
                    else:
                        if bairro is None:
                            bairro = item
                        else:
                            bairro += ', ' + item
            return rua, numero, bairro, cidade

        # Caminho do arquivo CSV onde os dados serão salvos
        csv_file = 'data.csv'

        # Abrir o arquivo CSV em modo de escrita
        with open(csv_file, "w", newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            # Escrever os cabeçalhos das colunas
            writer.writerow(['Título', 'Nome da Rua', 'Número', 'Bairro', 'Cidade', 'Preço', 'Área', 'Quartos', 'Banheiros', 'Garagens'])

            # Iterar sobre as páginas
            for i in range(160):
                try:
                    # Criando um objeto Request com o URL e o user-agent
                    req = Request(f'https://www.chavesnamao.com.br/imoveis-a-venda/sc-balneario-camboriu/?pg={i}', headers={'User-Agent': self.user_agent})

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
                            endereco = address_element.text.strip()
                            nome_rua, numero, bairro, cidade = separar_endereco(endereco)
                        else:
                            nome_rua, numero, bairro, cidade = '', '', '', ''
                        
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
                        writer.writerow([title, nome_rua, numero, bairro, cidade, price, area, quartos, banheiros, garagens])
                except IndexError:
                    print(f"Alcançou o final das páginas na página {i}. Finalizando...")
                    break  # Sai do loop quando alcança o final das páginas
                except Exception as e:
                    print(f"Ocorreu um erro: {e}")




# Função para converter uma linha para JSON
def to_json(row):
    # Definindo os nomes dos campos
    fields = ['Tipo', 'Endereço', 'Bairro', 'Numero', 'Cidade', 'Preço', 'Taxa', 'Área_m2', 'Quartos', 'Banheiros', 'Garagens']
    
    # Criando um dicionário para representar a linha
    json_row = {fields[i]: row[i] for i in range(len(fields))}
    
    # Convertendo o dicionário para JSON, garantindo que os caracteres acentuados sejam tratados corretamente
    return json.dumps(json_row, ensure_ascii=False)

# Criando o pipeline para a ingestão de dados
p_ingest = beam.Pipeline()

data_ingest = (
    p_ingest
    | "Executando a função Ingest" >> beam.Create([None])
    | "Processamento dos dados" >> beam.ParDo(IngestDoFn())
)

result_ingest = p_ingest.run()

# Esperando a conclusão da execução da ingestão de dados antes de prosseguir
result_ingest.wait_until_finish()

# Criando o pipeline para a transformação de dados
p_transform = beam.Pipeline()

data_transform = (
    p_transform
    | "Leitura dos dados" >> beam.io.ReadFromText('data.csv', skip_header_lines=1)
    | "Split Row" >> beam.Map(lambda row: row.split(',')) 
    | "Eliminando 'R$ ' da string" >> beam.Map(lambda cols: [col.replace('R$ ', '').replace('Cond.: ', '').replace(' Quartos', '').replace(' Quarto', '').replace(' Banheiros', '').replace(' Banheiro', '').replace(' Garagens', '').replace(' Garagem', '') for col in cols])
#   | "Salvando as taxas em uma nova coluna 6" >> beam.Map(lambda cols: [cols[i] for i in range(len(cols))] + [cols[5].split()[1]] if len(cols[5].split()) > 1 else [cols[i] for i in range(len(cols))] + [np.nan])
    | "Salvando as taxas em uma nova coluna" >> beam.Map(lambda cols: cols[:6] + [cols[5].split()[1] if len(cols[5].split()) > 1 else np.nan] + cols[6:])
    | "Eliminando taxa da coluna 6" >> beam.Map(lambda cols: [cols[i] if i != 5 else cols[5].split()[0] for i in range(len(cols))])
    | "Tratando os preços" >> beam.Map(lambda cols: [col[:-3] if index == 5 and col else col for index, col in enumerate(cols)])
    | "Elinando pontos em preços" >> beam.Map(lambda cols: [col.replace('.', '') if index == 5 else col for index, col in enumerate(cols)])
    | "Elinando pontos em taxas" >> beam.Map(lambda cols: [str(col).replace('.', '') if index == 6 else col for index, col in enumerate(cols)])
    | "Salvando número em uma nova coluna" >> beam.Map(lambda cols: cols[:4] + [cols[3].split()[0] if len(cols[3].split()) > 1 else np.nan] + cols[4:])
    | "Eliminando númeoro da coluna 4" >> beam.Map(lambda cols: [cols[i] if i != 3 else ' '.join(cols[3].split()[1:]) if len(cols) >= 2 and len(cols[3].split()) > 1 else cols[i] for i in range(len(cols))])
    | "Eliminando coluna 2" >> beam.Map(lambda cols: [cols[i] for i in range(len(cols)) if i != 2])
    | "Converting to JSON" >> beam.Map(to_json)
    | "Print Transformed Rows" >> beam.io.WriteToText('data/data_tratado.json')
    #| "Print Transformed Rows" >> beam.Map(print) 
)

p_transform.run()

