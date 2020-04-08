from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.request import urlopen
import selenium as sl
from selenium import webdriver
from pymongo import MongoClient 
import psycopg2 as pg2

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
    driver = webdriver.Firefox(executable_path='/home/karl/PythonDirectories/geckodriver-v0.26.0-linux64/geckodriver')
    driver.get(formattedUrl)
    driver.implicitly_wait(20)

    #html = driver.page_source
    #html = requests.get(formattedUrl).text
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    temp = soup.find_all('div', {'class':'summary-table'})
    #print(temp)
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

    driver.close()
    #print('''INSERT INTO weather 
    #VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'''%(Station, ActHighTemp, HistAvgHighTemp, ActLowTemp, HisAvgLowTemp, ActDayAvgTemp, HistAvgAvgTemp, ActPrecip, HistAvgPrecip, current_date, current_date.year, current_date.month, current_date.day))
    cur.execute('''INSERT INTO weather (station, actualhightemp, histavghightemp, actuallowtemp, histavglowtemp, actualdailyavgtemp, histavgavgtemp, actualprecip, histavgprecip, year, month, day) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);''', (Station, ActHighTemp, HistAvgHighTemp, ActLowTemp, HisAvgLowTemp, ActDayAvgTemp, HistAvgAvgTemp, ActPrecip, HistAvgPrecip, current_date.year, current_date.month, current_date.day))
    conn.commit()
    current_date +=timedelta(days=1)