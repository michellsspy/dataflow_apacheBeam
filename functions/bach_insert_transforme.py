import apache_beam as beam
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import csv
import re
import numpy as np

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

p1 = beam.Pipeline()

#tempo_atrasos = (
#    p1
#    | "Executando a função Ingest" >> beam.Create([None])  # Passing a dummy element
#    | "Processamento dos dados" >> beam.ParDo(IngestDoFn())
#) 

data_transform = (
    p1
    | "Leitura dos dados" >> beam.io.ReadFromText('data.csv', skip_header_lines=1)
    | "Split Row" >> beam.Map(lambda row: row.split(',')) 
    | "Eliminando 'R$ ' da string" >> beam.Map(lambda cols: [col.replace('R$ ', '').replace('Cond.: ', '').replace(' Quartos', '').replace(' Quarto', '').replace(' Banheiros', '').replace(' Banheiro', '').replace(' Garagens', '').replace(' Garagem', '') for col in cols])
    #| "Salvando as taxas em uma nova coluna 6" >> beam.Map(lambda cols: [cols[i] for i in range(len(cols))] + [cols[5].split()[1]] if len(cols[5].split()) > 1 else [cols[i] for i in range(len(cols))] + [np.nan])
    | "Salvando as taxas em uma nova coluna" >> beam.Map(lambda cols: cols[:5] + [cols[5].split()[1] if len(cols[5].split()) > 1 else np.nan] + cols[5:])
    | "Eliminando taxa da coluna 6" >> beam.Map(lambda cols: [cols[i] if i != 6 else cols[6].split()[0] for i in range(len(cols))])
    | "Tratando os preços" >> beam.Map(lambda cols: [col[:-3] if index == 6 and col else col for index, col in enumerate(cols)])
    | "Elinando pontos em preços" >> beam.Map(lambda cols: [col.replace('.', '') if index == 6 else col for index, col in enumerate(cols)])
    | "Elinando pontos em taxas" >> beam.Map(lambda cols: [str(col).replace('.', '') if index == 5 else col for index, col in enumerate(cols)])
    #| "Processamento da coluna 3" >> beam.Map(lambda cols: [cols[0], cols[1], cols[2].split()[0], ' '.join(cols[2].split()[1:])] if len(cols[2].split()) > 2 else cols)
    #| "Substituindo ponto na última coluna" >> beam.Map(lambda cols: [str(col).replace('.', '') if index == len(cols) - 1 else col for index, col in enumerate(cols)])
    | "Print Transformed Rows" >> beam.Map(print)  
)

    #| "Leitura dos dados" >> beam.io.ReadFromText('data.csv', skip_header_lines=1)
    #| "Split Row" >> beam.Map(lambda row: row.split(',')) 
    #
    #| "Salvando as taxas em uma nova coluna" >> beam.Map(lambda cols: cols[:5] + [cols[5].split()[1] if len(cols[5].split()) > 1 else np.nan] + cols[5:])


#    df = pd.read_csv('/home/michel/Documentos/Projetos_Portifolio/dataflow_apacheBeam/data.csv')
#
#    # Aplicar a lógica usando lambda e apply
#    df['Número'], df['Bairro'] = zip(*df.apply(lambda row: (row['Bairro'].split()[0], ' '.join(row['Bairro'].split()[1:])) if isinstance(row['Bairro'], str) and len(row['Bairro'].split()) > 1 else (row['Número'], row['Bairro']), axis=1))
#
#    # Converter 'Número' para int64
#    df['Número'] = pd.to_numeric(df['Número'], errors='coerce').astype('Int64')
#
#    df.insert(6, 'Taxa Cond', df.apply(lambda row: row['Preço'].split()[4] if isinstance(row['Preço'], str) and len(row['Preço'].split()) > 3 else None, axis=1))
#    df['Preço'] = df.apply(lambda row: row['Preço'].split()[1] if isinstance(row['Preço'], str) and len(row['Preço'].split()) > 2 else row['Preço'], axis=1)
#
#    df['Preço'] = df['Preço'].str.replace('R$ ', '')
#    df['Preço'] = df['Preço'].apply(lambda x: x[:-4] if x else '0')
#    df['Preço'] = df['Preço'].str.replace('.', '')
#
#    df['Preço'] = df['Preço'].apply(lambda x: float(x.replace('R$', '').replace('.', '').replace(',', '.')) if str(x).replace('.', '').isdigit() else np.nan)
#
#    df['Taxa Cond'] = df['Taxa Cond'].str.replace('.', '')
#    df['Taxa Cond'] = df['Taxa Cond'].replace('Cond.: R\$ ', '', regex=True).astype(float)
#    df.insert(7, 'Total Mensal', df.apply(lambda row: row['Preço'] + row['Taxa Cond'], axis=1))
#
#    df['Quartos'] = df['Quartos'].astype('str').apply(lambda x: x.split()[0])
#    df['Banheiros'] = df['Banheiros'].astype('str').apply(lambda x: x.split()[0])
#    df['Garagens'] = df['Garagens'].astype('str').apply(lambda x: x.split()[0])
#
#    df['Quartos'] = df['Quartos'].apply(lambda x: int(x) if str(x).isnumeric() else 0)
#    df['Banheiros'] = df['Banheiros'].apply(lambda x: int(x) if str(x).isnumeric() else 0)
#    df['Garagens'] = df['Garagens'].apply(lambda x: int(x) if str(x).isnumeric() else 0)
#
#    df['Área'] = df['Área'].apply(lambda x: float(x) if str(x).isnumeric() else 0)
#    df['Quartos'] = df['Quartos'].astype('Int64')
#    df['Banheiros'] = df['Banheiros'].astype('Int64')
#    df['Garagens'] = df['Garagens'].astype('Int64')
#
#    df.to_csv('data/data_transform.csv')

p1.run()
