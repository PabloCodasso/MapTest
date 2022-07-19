import requests
from bs4 import BeautifulSoup as bsp

result = requests.get('https://docs.google.com/')
print('Status code: %s' % result.status_code)
print('HTTP text: \n')
print(result.text)
