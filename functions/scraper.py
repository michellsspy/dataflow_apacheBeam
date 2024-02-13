from urllib.request import urlopen, Request
from bs4 import BeautifulSoup

# Definindo o user-agent que você deseja usar
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'

# Criando um objeto Request com o URL e o user-agent
req = Request('https://www.chavesnamao.com.br/imoveis/brasil', headers={'User-Agent': user_agent})

# Fazendo a solicitação usando urlopen com o objeto Request
html = urlopen(req)

# Parseie o HTML com BeautifulSoup
bs = BeautifulSoup(html.read(), 'html.parser')

# Imprimindo o HTML parseado
print(bs)
