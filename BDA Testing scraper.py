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


def extract_reviews(input_string):
    if isinstance(input_string, (list, set)):
        input_string = ' '.join(str(element) for element in input_string)
    
    rating_pattern = r'(\d+) out of 5 stars'

    rating_match = re.search(rating_pattern, input_string)

    ram = rating_match.group(1) if rating_match else '8'

    return ram

def extract_specs(input_string):
    if isinstance(input_string, (list, set)):
        input_string = ' '.join(str(element) for element in input_string)
    
    ram_pattern = r'(\d+)GB RAM'
    storage_pattern = r'(\d+)GB Storage'

    ram_match = re.search(ram_pattern, input_string)
    storage_match = re.search(storage_pattern, input_string)

    ram = ram_match.group(1) if ram_match else '8'
    storage = storage_match.group(1) if storage_match else '64'

    return ram, storage

def extract_pricesflipkart(input_string):
    if isinstance(input_string, (list, set)):
        input_string = ' '.join(str(element) for element in input_string)
    
    price_pattern = r'â‚¹\s*([\d,]+)'  
    price_match = re.search(price_pattern, input_string)

    price = price_match.group(1).replace(',', '') if price_match else '8000'

    return price





for i in range(2, 600):
     amaznurl="https://www.amazon.in/s?k=mobile&page={i}&crid=35KGP6NVVEGY7&qid=1723898222&sprefix=%2Caps%2C194&ref=sr_pg_"+str(i)
     r1=requests.get(amaznurl)
     soup1 = BeautifulSoup(r1.text, "lxml")
     amazreviews=soup1.find_all("span", class_="a-icon-alt")
     amaznames= soup1.find_all("span",class_="a-size-medium a-color-base a-text-normal")
     amazsprice1=soup1.find_all("span",class_="a-price-whole")   



     for i in amazreviews:
         amazreviews1= i.text
         extractedreviews= extract_reviews(amazreviews1)
         reviews.append(extractedreviews)


     for i in amazsprice1:
         price2= i.text
         prices.append(price2)

     for i in amaznames:
         amaznname= i.text
         specsamazram, specsforamazstorage1= extract_specs(amaznames)
         ramspecs.append(specsamazram)
         storagespecs.append(specsforamazstorage1)
         product_name.append(amaznname)

print(len(reviews))
print(len(prices))
print(len(product_name))
        

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
         print(specsforram)
         print(specsforstorage)
         specs1ram.append(specsforram)
         storagespecs.append(specsforstorage)

        
    for i in price1:
         price= i.text
         extractedflipkartprices= extract_pricesflipkart(price)
         prices.append(extractedflipkartprices)


    for i in names:
         name= i.text
         product_name.append(name)


print("product names are: ",len(product_name))
print("prices are: ",len(prices))
print("reviews are: ",len(reviews))
print('ram specs length :', len(ramspecs))
print('Storage specs length :', len(storagespecs))

max_length = max(len(product_name), len(prices), len(reviews), len(ramspecs), len(storagespecs))

product_name.extend([None] * (max_length - len(product_name)))
prices.extend([None] * (max_length - len(prices)))
reviews.extend([None] * (max_length - len(reviews)))
ramspecs.extend([None] * (max_length - len(ramspecs)))
storagespecs.extend([None] * (max_length - len(storagespecs)))


df= pd.DataFrame({"Mobile Name": product_name,"Prices":prices, "Reviews": reviews, "RAM Specifications": ramspecs, "Storage Specifications": storagespecs})
df.to_csv("Scrapped_Data_for_allinfo.csv")