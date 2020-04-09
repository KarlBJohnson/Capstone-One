from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import selenium as sl
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient 
import psycopg2 as pg2
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import time

options = Options()
options.add_argument("--headless")
options.add_argument('--no-sandbox')
options.add_argument('--disable-gpu')
chrome_options.add_argument('--lang=en_US') 
Station = "KAUS"
current_date = datetime(year=1996, month=1, day=1)
end_date = datetime(year=2009, month=1, day=1)
conn = MongoClient()
db = conn.database
coll = db.collection[Station]
conn = pg2.connect(host = 'localhost', user='postgres', password='Password', dbname = "weatherdata")
cur = conn.cursor()
#cur.execute('CREATE TABLE Weather (Entryid int NOT NULL AUTO_INCREMENT, Station VARCHAR(255), ActualHighTemp int, HistAvgHighTemp int, ActualLowTemp into, HistAvgLowTemp int, ActualDailyAvgTemp int, HistAvgAvgTemp int, ActualPrecip float, HistAvgPrecip float, Date datetime, Year int, Month int, Day int, PRIMARY KEY(Entryid))')
#db.insert['coll']("Station", "Actual High Temp", "HistAvg High Temp", "Actual Low Temp", "HistAvg Low Temp", "Actual Daily Avg Temp","HistAvg Average Temp", "Actual Precip", "HistAvg Precip", "Actual Wind", "HistAvg Wind", "Date", "Year","Month", "Day")
url = 'https://www.wunderground.com/history/daily/{}/date/{}-{}-{}'
while current_date != end_date:
    formattedUrl = url.format(Station, current_date.year, current_date.month, current_date.day)
    driver = webdriver.Chrome(options=options)
    driver.get(formattedUrl)
    
    time.sleep(10)
    driver.implicitly_wait(5)
    #html = driver.page_source
    #html = requests.get(formattedUrl).text
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    temp = soup.find_all('div', {'class':'summary-table'})
    print(temp)
    weather_data = []
    for el in temp:
        weather_data.append(el.find_all('td', {'class': 'ng-star-inserted'}))
    weather_data = weather_data[0]
    try:
        coll.insert_one({_id: "{}-{}-{}".format(current_date.year, current_date.month, current_date.day), _item:weather_data})
    except:
        pass
    try: 
        ActHighTemp = int(weather_data[3].text)
    except:
        ActHighTemp = None
    #print(ActHighTemp)
    try:
        HistAvgHighTemp = int(weather_data[4].text)
    except:
        HistAvgHighTemp = None
    #print(HistAvgHighTemp)
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
        ActDayAvgTemp = None
    try:
        HistAvgAvgTemp = float(weather_data[10].text)
    except:
        HistAvgAvgTemp = None
    try:
        ActPrecip = float(weather_data[15].text)
    except:
        ActPrecip = None
    try:
        HistAvgPrecip = float(weather_data[16].text)
    except:
        HistAvgPrecip = None

    driver.close()
    cur.execute('''INSERT INTO weather (station, actualhightemp, histavghightemp, actuallowtemp, histavglowtemp, actualdailyavgtemp, histavgavgtemp, actualprecip, histavgprecip, year, month, day) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''', (Station, ActHighTemp, HistAvgHighTemp, ActLowTemp, HisAvgLowTemp, ActDayAvgTemp, HistAvgAvgTemp, ActPrecip, HistAvgPrecip, current_date.year, current_date.month, current_date.day))
    print(ActDayAvgTemp, ActHighTemp, ActLowTemp, ActPrecip)
    conn.commit()
    current_date +=timedelta(days=1)