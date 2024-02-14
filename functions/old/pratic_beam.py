import apache_beam as beam

p1 = beam.Pipeline()

tempo_atrasos = (
    p1
    | "Importa os dados" >> beam.io.ReadFromText('data/voos_sample.csv', skip_header_lines = 1)
    | "Separar por vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com atraso" >> beam.Filter(lambda record: int(record[8]) > 0)
    | "Criar os pares" >> beam.Map(lambda reacord: (reacord[4], int(reacord[8])))
    | "Somar e agrupar por Key" >> beam.CombinePerKey(sum)
)

qtd_atrasos = (
    p1
    | "Importando os dados" >> beam.io.ReadFromText('data/voos_sample.csv', skip_header_lines = 1)
    | "Separar os dados por vírgula" >> beam.Map(lambda record: record.split(','))
    | "Pegar voos com atraso 2" >> beam.Filter(lambda record: int(record[8]) > 0)
    | "Foramndo os pares" >> beam.Map(lambda record: (record[4], int(record[8])))
    | "Agrupar por Key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos': qtd_atrasos, 'Tempo_Atrasos': tempo_atrasos}
    | "Grouo By" >> beam.CoGroupByKey()
    | "Print" >> beam.Map(print)
)
p1.run()