import pandas as pd
import numpy as np

df = pd.read_csv('/home/michel/Documentos/Projetos_Portifolio/dataflow_apacheBeam/data.csv')

# Aplicar a lógica usando lambda e apply
df['Número'], df['Bairro'] = zip(*df.apply(lambda row: (row['Bairro'].split()[0], ' '.join(row['Bairro'].split()[1:])) if isinstance(row['Bairro'], str) and len(row['Bairro'].split()) > 1 else (row['Número'], row['Bairro']), axis=1))

# Converter 'Número' para int64
df['Número'] = pd.to_numeric(df['Número'], errors='coerce').astype('Int64')

df.insert(6, 'Taxa Cond', df.apply(lambda row: row['Preço'].split()[4] if isinstance(row['Preço'], str) and len(row['Preço'].split()) > 3 else None, axis=1))
df['Preço'] = df.apply(lambda row: row['Preço'].split()[1] if isinstance(row['Preço'], str) and len(row['Preço'].split()) > 2 else row['Preço'], axis=1)

df['Preço'] = df['Preço'].str.replace('R$ ', '')
df['Preço'] = df['Preço'].apply(lambda x: x[:-4] if x else '0')
df['Preço'] = df['Preço'].str.replace('.', '')

df['Preço'] = df['Preço'].apply(lambda x: float(x.replace('R$', '').replace('.', '').replace(',', '.')) if str(x).replace('.', '').isdigit() else np.nan)

df['Taxa Cond'] = df['Taxa Cond'].str.replace('.', '')
df['Taxa Cond'] = df['Taxa Cond'].replace('Cond.: R\$ ', '', regex=True).astype(float)
df.insert(7, 'Total Mensal', df.apply(lambda row: row['Preço'] + row['Taxa Cond'], axis=1))

df['Quartos'] = df['Quartos'].astype('str').apply(lambda x: x.split()[0])
df['Banheiros'] = df['Banheiros'].astype('str').apply(lambda x: x.split()[0])
df['Garagens'] = df['Garagens'].astype('str').apply(lambda x: x.split()[0])

df['Quartos'] = df['Quartos'].apply(lambda x: int(x) if str(x).isnumeric() else 0)
df['Banheiros'] = df['Banheiros'].apply(lambda x: int(x) if str(x).isnumeric() else 0)
df['Garagens'] = df['Garagens'].apply(lambda x: int(x) if str(x).isnumeric() else 0)

df['Área'] = df['Área'].apply(lambda x: float(x) if str(x).isnumeric() else 0)
df['Quartos'] = df['Quartos'].astype('Int64')
df['Banheiros'] = df['Banheiros'].astype('Int64')
df['Garagens'] = df['Garagens'].astype('Int64')

df.to_csv('data/data_transform.csv')