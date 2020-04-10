from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pprint
import selenium as sl
from selenium import webdriver
import psycopg2 as pg2

def scrape(Station, current_date, end_date):
    #Note that this scaper requires you to have postgres on your device with the table and fields already created
    conn = pg2.connect(host = 'localhost', user='postgres', password='Password', dbname = "weatherdata")
    cur = conn.cursor() #Establishing connection with database and setting up cursor
    url = 'https://www.wunderground.com/history/daily/{}/date/{}-{}-{}'
    while current_date != end_date: #iterating from starting date to end date
        formattedUrl = url.format(Station, current_date.year, current_date.month, current_date.day)
        driver = webdriver.Firefox(executable_path='/home/karl/PythonDirectories/geckodriver-v0.26.0-linux64/geckodriver')
        driver.get(formattedUrl)
        driver.implicitly_wait(13)#Because of how Wunderground is configured, the web page loads empty tables, and then populates it from an API
        if current_date.day ==1: #We're using selenium and pausing for 13 seconds to give web page time to load
            print(current_date)#Printing out the first day of each month to make progress measurable
        soup = BeautifulSoup(driver.page_source, 'html.parser')#Taking my html data and passing it into bs4
        temp = soup.find_all('div', {'class':'summary-table'}) #Finding the summary table
        weather_data = []
        for el in temp:
            weather_data.append(el.find_all('td', {'class': 'ng-star-inserted'}))#Finding the individual datapionts I want
        weather_data = weather_data[0]#Small quirk of how I instantiated weather_data to ensure I'm using the correct summary table
        #Monsterous block of try excepts to ensure that null values don't raise an error when attempting to type cast them
        try: 
            ActHighTemp = int(weather_data[3].text)
        except:
            ActHighTemp = None
        try:
            HistAvgHighTemp = int(weather_data[4].text)
        except:
            HistAvgHighTemp = None
        try:
            ActLowTemp = int(weather_data[6].text)
        except:
            ActLowTemp = None
        try:
            HisAvgLowTemp = int(weather_data[7].text)
        except:
            HisAvgLowTemp = None
        try:
            ActDayAvgTemp = float(weather_data[9].text)
        except:
            HisAvgLowTemp = None
        try:
            HistAvgAvgTemp = float(weather_data[10].text)
        except:
            HisAvgAvgTemp = None
        try:
            ActPrecip = float(weather_data[15].text)
        except:
            ActPrecip = None
        try:
            HistAvgPrecip = float(weather_data[16].text)
        except:
            HistAvgPrecip = None

        driver.close() #Close the webpage after scraping
        cur.execute('''INSERT INTO weather (station, actualhightemp, histavghightemp, actuallowtemp, histavglowtemp, actualdailyavgtemp, histavgavgtemp, actualprecip, histavgprecip, year, month, day) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''', (Station, ActHighTemp, HistAvgHighTemp, ActLowTemp, HisAvgLowTemp, ActDayAvgTemp, HistAvgAvgTemp, ActPrecip, HistAvgPrecip, current_date.year, current_date.month, current_date.day))
        conn.commit() #Push your data into the SQL database
        current_date +=timedelta(days=1) #Move to the next day
            