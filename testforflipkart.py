import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
product_name=[]
prices=[]
reviews=[]
ramspecs=[]
storagespecs=[]






def extract_specsflipkart(input_string):
    if isinstance(input_string, (list, set)):
        input_string = ' '.join(str(element) for element in input_string)
    
    ram_pattern = r'(\d+) GB RAM'
    storage_pattern = r'(\d+) GB ROM'

    ram_match = re.search(ram_pattern, input_string)
    storage_match = re.search(storage_pattern, input_string)

    ram = ram_match.group(1) if ram_match else '8'
    storage = storage_match.group(1) if storage_match else '64'

    return ram, storage

for i in range(2,45):
    url = "https://www.flipkart.com/search?q=mobile&otracker=search&otracker1=search&marketplace=FLIPKART&as-show=on&as=off&page="+str(i)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")





    box=soup.find("div",class_="DOjaWF gdgoEp")
    names= soup.find_all("div",class_="KzDlHZ")
    price1=soup.find_all("div",class_='Nx9bqj _4b5DiR')
    specs1ram=soup.find_all("div",class_="_6NESgJ")
    specs1=soup.find_all("li",class_="J+igdf")
    review= soup.find_all("div",class_="XQDdHH")

    for i in review:
         review1= i.text         
         reviews.append(review1)

    for i in specs1:
         spec= i.text
         specsforram,specsforstorage =extract_specsflipkart(spec)
         ramspecs.append(specsforram)
         storagespecs.append(specsforstorage)

        
    for i in price1:
         price= i.text
         prices.append(price)


    for i in names:
         name= i.text
         product_name.append(name)

print("product names are: ",len(product_name))
print("prices are: ",len(prices))
print("reviews are: ",len(reviews))
print('ram specs length :', len(ramspecs))
print('Storage specs length :', len(storagespecs))
print('ram specs length :', ramspecs)
print('Storage specs length :', storagespecs)
