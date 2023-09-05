import requests
import random
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import csv
import os
import re
import psycopg2
from datetime import datetime, timedelta
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from multiprocessing import Pool
from tqdm import tqdm

# postgis connection
# con = psycopg2.connect(database="pandora", 
#                        user="data_reader", 
#                        password="Economics1",
#                        host="10.10.50.46")

# api_data = f"""SELECT place_id, 
#                 vicinity, 
#                 primary_type
# FROM land_use.google_places_building_relations_rnsw """

# # establish SQL connection to get place_id data

# place_ids = pd.read_sql(api_data, con)

place_ids = pd.read_csv('regional_place_ids.csv')

# process address column

place_ids['vicinity'] = place_ids.vicinity.str.replace(r"\,", "", regex=True)
place_ids['vicinity'] = place_ids.vicinity.str.replace(r"\d+\/", "", regex=True)
address_components = place_ids.vicinity.str.split(" ", expand=True)
place_ids = place_ids.join(address_components)

# after splitting out the addresses based on whitespace, a small subset of rows have as many as 36 components - these are highly unlikely to able to be used in an Airbnb URL, so filter out long addresses (anything with 7 or more components). Only lose ~700 of the 14,000 records

df = place_ids[place_ids.iloc[:, 10:].isna().all(axis=1)]
df = df.iloc[:, 0:10]
df = df.drop_duplicates(subset='vicinity').sort_values(by='place_id')

# add dashes to easily pass thorugh address into URL

df['vicinity'] = df.vicinity.str.replace(" ", "-")
df.reset_index(inplace=True)

df_test = df.iloc[0:1000, :]
print(df_test)

class Parser:
    
    def __init__(self, addresses, place_ids):
        self.addresses = addresses # pass through list of addresses
        self.place_ids = place_ids  # pass through list of place ids, corresponding to the addresses - need to get this from goog api data

    #collect the url of each search results page. We need this so that for every url, we can extract the listing ID, which will then enable us to get the URL for every listing to scrape    
    def get_urls(self):
        checkin = datetime.now() + timedelta(days=7)
        checkin_month = checkin.strftime('%m')
        checkin_date = checkin.strftime('%d')
        checkout = datetime.now() + timedelta(days=8)
        checkout_month = checkout.strftime('%m')
        checkout_date = checkout.strftime('%d')
        
        base_urls = []
        url_list = []
        address_list = []
        
        for count, address in enumerate(self.addresses):
            base_url = f"https://www.airbnb.com.au/s/{address}--New-South-Wales/homes?adults=2&place_id={self.place_ids[count]}&children=1&checkin=2023-{checkin_month}-{checkin_date}&checkout=2023-{checkout_month}-{checkout_date}"
            base_urls.append(base_url)
            address_list.append(address)
            
        url_list = url_list + base_urls
            
        for i in tqdm(range(len(base_urls))):
            # for j in range(0, 14): # each page of results has 15 listing results to collect the url for
            try:
                url = base_urls[i]
                r = requests.get(url)
                soup = BeautifulSoup(r.text, "lxml")
                elements = soup.find_all("a", class_="l1ovpqvx c1ackr0h dir dir-ltr") # this gets the hrefs for all pages  
                np = []
                for element in elements:
                    href = element['href']       
                    np.append(href)  
                url = ["https://www.airbnb.com.au" + element for element in np]
                url_list.extend(url)
            except Exception as e:
                print("Exception occurred:", e)
                print('Get URL error')
                url_list.append('No valid URL')

        urls = {'url': url_list}
        
        url_df = pd.DataFrame(urls)
        url_df['idx'] = url_df.index
        url_df.drop('idx', axis=1, inplace=True)  
        url_df = url_df[url_df.url != 'No valid URL']
        url_df.drop_duplicates(subset=['url'], inplace=True)
        self.url_df = url_df

        print(url_df)
        
    # use listing ID's to get URL for every listing collected    
    def get_listing_ids(self, urls):

        checkin = datetime.now() + timedelta(days=7)
        checkin_month = checkin.strftime('%m')
        checkin_date = checkin.strftime('%d')
        checkout = datetime.now() + timedelta(days=8)
        checkout_month = checkout.strftime('%m')
        checkout_date = checkout.strftime('%d')
        
        listings = []

        for url in urls:
            try:
                r = requests.get(url)
                soup = BeautifulSoup(r.text, "lxml")
                listing_suffix = soup.find_all("a", class_="rfexzly dir dir-ltr")
                
                for element in listing_suffix:
                    id = element.get("target")
                    result = re.search('(?<=_)(.*)', id)
                    if result:
                        listing_id = result.group(1)
                        listing_url = f"https://www.airbnb.com.au/rooms/{listing_id}?source_impression_id=p3_1686404449_uYtjrfB0YIsq6M8E&guests=1&adults=1&check_in=2023-{checkin_month}-{checkin_date}&check_out=2023-{checkout_month}-{checkout_date}"
                        listings.append(listing_url)
                
            except Exception as e:
                # print("Exception occurred:", e)
                continue

        listings_set = set()

        for i in listings:
            listings_set.add(i)
        
        self.listings = listings_set
        print(listings_set)
        return self.listings
    
    # scrape property id and price for each URL
    def extract_values(self, urls):
        ids_list = []
        prices_list = []

        for url in tqdm(urls):
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
            wait_time = random.randint(30, 60)
            driver.set_page_load_timeout(wait_time)
            try:
                driver.get(url)
                wait = WebDriverWait(driver, wait_time)

                wait.until(EC.visibility_of_element_located((By.XPATH, "//li[@class='f19phm7j dir dir-ltr']")))
                daily_rates = driver.find_elements(By.XPATH, "//span[@class='_tyxjp1']")
                if len(daily_rates) >= 2:
                    daily_rate = daily_rates[1].text
                else:
                    daily_rate = daily_rates[0].text
               
                stra_id = driver.find_element(By.XPATH, "//li[@class='f19phm7j dir dir-ltr']")
                stra_id = stra_id.text

                stra_id = re.search('(?<=:\s)(.*)', stra_id)
                stra_id = stra_id.group(1)

                ids_list.append(stra_id)
                prices_list.append(daily_rate)
                print(stra_id)

            except Exception as e:
                print('Value extraction error:', e)
           
            driver.quit()
            time.sleep(wait_time)

        self.id_list = ids_list
        self.price_list = prices_list

        return self.id_list, self.price_list

    # multi-processor for collecting listing ids
    def listing_url_processor(self):

        # Set number of processes
        num_processes = os.cpu_count() // 2

        # iteratevely grab listing ID's for 100 URL's at a time
        count = 0
        property_urls = []

        page_urls = self.url_df['url']
        # for i in range(len(self.url_df) // 100):
        #     page_urls = self.url_df.iloc[count:count+99]['url']
        #     count += 100
        
        try:
            chunk_size = len(page_urls) // num_processes
            chunks = [page_urls[j:j+chunk_size] for j in range(0, len(page_urls), chunk_size)]
            pool = Pool(processes=num_processes)
            property_urls_setlist = pool.map(self.get_listing_ids, chunks)

            for i in property_urls_setlist:
                property_urls.extend(i)

            # Wait for all processes to complete
            pool.close()
            pool.join()
        
        except Exception as e:
            print('Exception occurred:', e)
            print('Listing URL error')
            
        self.property_urls = property_urls
            
    # multi processor for scraping property ids and prices
    def data_processor(self):
        
        num_processes = os.cpu_count() // 2

        # iteratevely grab listing ID's for 100 URL's at a time
        # count = 0
        # for i in range((self.property_urls) // 100):
        #     property_urls = self.property_urls.iloc[count:count+99]
        #     count += 100

        try:
            chunk_size = len(self.property_urls) // num_processes
            chunks = [self.property_urls[j:j+chunk_size] for j in range(0, len(self.property_urls), chunk_size)]
            pool = Pool(processes=num_processes)
            results = pool.map(self.extract_values, chunks)
            pool.close()
            pool.join()
            
            ids = []
            prices = []

            for result in results:
                ids.extend(result[0])
                prices.extend(result[1])

            self.ids = ids
            self.prices = prices
            self.results = results

        except Exception as e:
            print('Data processor error:', e)

    
    def dataframe_save(self):
        try:
            data = pd.DataFrame({'property_id': self.ids, 'price': self.prices})
            data.to_csv('airbnb_scraped_new.csv')
        except Exception as e:
            print(e)
        try:
            unprocessed = pd.DataFrame({'output': self.results})
            unprocessed.to_csv('unprocessed_scrape.csv')
        except:
            print('Unprocessed data failure')
        
    def parse(self):
        self.get_urls()
        self.listing_url_processor()
        self.data_processor()
        self.dataframe_save()

# test addresses
# addresses = df['vicinity'].tolist()
# place_ids = df['place_id'].tolist()

addresses = df_test.vicinity.tolist()
place_ids = df_test.place_id.tolist()

# addresses = ['Cherrybrook', 'Beacon-Hill']
# place_ids = ['ChIJj07T3tSgEmsR8LAyFmh9AQU', 'ChIJgSME6waqEmsR8KgyFmh9AQU']

if __name__ == "__main__":
    new_parser = Parser(addresses, place_ids)
    t0 = time.time()
    new_parser.parse()
    print(time.time() - t0)