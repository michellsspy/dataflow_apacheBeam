from bs4 import BeautifulSoup as bs
import pandas as pd
import pyarrow as pa
import apache_beam as beam

p1 = beam.Pipeline()

data_voos = (
p1
    | "Importando os dados" >> beam.io.ReadFromText("/home/michel/Documentos/Projetos_Portifolio/dataflow_apacheBeam/data/voos_sample.csv", skip_header_lines = 1)
    | "Separar por vírgula" >> beam.Map(lambda record: record.split(','))
#    | "Mostrar na tela" >> beam.Map(print)
    | "Grava a saída" >> beam.io.WriteToText("/home/michel/Documentos/Projetos_Portifolio/dataflow_apacheBeam/data/saida_dados.txt")
)

p1.run()


# Site alvo:
# https://www.chavesnamao.com.br/imoveis-a-venda/sc-balneario-camboriu/?pg=6