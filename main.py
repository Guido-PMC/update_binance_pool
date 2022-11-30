from binance.spot import Spot
import json
import requests
import time
from datetime import datetime
from datetime import timedelta
import os
import yfinance as yf
import pandas as pd
import urllib
import schedule
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import *
from oauth2client.service_account import ServiceAccountCredentials

credenciales = os.environ['CREDS']


def getSheetsDataFrame(sheet, worksheet):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credenciales, scope)
    client = gspread.authorize(creds)
    work_sheet = client.open(sheet)
    sheet_instance = work_sheet.worksheet(worksheet)
    records_data = sheet_instance.get_all_records()
    return (pd.DataFrame.from_dict(records_data))


def zabbix_push(puid, key, value):
    stream = os.popen(f"zabbix_sender -z '54.92.215.92'    -s {puid} -k application.{key} -o {str(value)}")
    output = stream.read()
    #print(output)

#######UPDATE BTC
def update_btc(puid):
    BTC_Ticker = yf.Ticker("BTC-USD")
    BTC_Data = BTC_Ticker.history(period="1D")
    BTC_Value = BTC_Data['High'].loc[BTC_Data.index[0]]
    zabbix_push(puid, "btc_price", BTC_Value)


#UPDATE USD
def update_usd(puid):
    url = "https://dolarhoy.com/cotizaciondolarblue"
    html = urllib.request.urlopen(url)
    soup = BeautifulSoup(html,"html.parser")
    tags = soup.find_all("div",class_="value")
    precios = list()
    for tag in tags:
        precios.append(tag.contents[0])
    buy = float(precios[0].replace("$",""))
    sell = float(precios[1].replace("$",""))
    print(f"Valor del dolar buy = {buy}")
    zabbix_push(puid, "dolar_blue", buy)


#INVERSION INICIAL
def update_inversionInicial(puid,inversion):
    stream = os.popen(f"zabbix_sender -z '54.92.215.92'    -s {puid} -k application.inversion_inicial -o {str(inversion)}")
    output = stream.read()


def job():
    username = "bigsur1"
    usersDF = getSheetsDataFrame("Grafana-Mining-BD","BD1")#.to_dict()
    usuariosPoolList = []
    nombreClienteList = []
    valorKhwList = []
    inversionInicialList = []
    revShareList = []
    baseTotalminedList = []

    for usuariosPool in usersDF["usuariosPool"]:
        usuariosPoolList.append(usuariosPool)
    for nombreCliente in usersDF["nombreCliente"]:
        nombreClienteList.append(nombreCliente)
    for valorKhw in usersDF["valorKhw"]:
        valorKhwList.append(valorKhw)
    for inversionInicial in usersDF["inversionInicial"]:
        inversionInicialList.append(inversionInicial)
    for revShare in usersDF["revShare"]:
        revShareList.append(revShare)
    for baseTotalmined in usersDF["baseTotalmined"]:
        baseTotalminedList.append(baseTotalmined)


    toTerahash = 1000000000000


    client = Spot()
    client = Spot(key='Qu7J7lsjEp6Pnw9fHFV51qH24hnjuDoiv2dHwIaKs008ZJAPCisMzE47ferfqOYM', secret='qaY6EQur8pJabWCRpxkGvie0i2dBJf8WVONi8oybk3TvLQUcH1B96P1Lxty4Wf6n')

    for x in range(0,len(usuariosPoolList)):
        print(usuariosPoolList[x])
        json1 = (client.mining_statistics_list(algo="sha256", userName=usuariosPoolList[x]))
        #pp.pprint(json1)
        print(f"Username:{json1['data']['userName']} ")
        print(f"Hashrate:{(str(float(json1['data']['dayHashRate'])/toTerahash))} ")
        print(f"Today Mined:{json1['data']['profitToday']['BTC']} ")
        print(f"Yesterday Mined:{json1['data']['profitYesterday']['BTC']} ")
        json2 = (client.mining_earnings_list(algo="sha256", userName=usuariosPoolList[x]))
        #pp.pprint(json2)
        print(f"Total mined {float(format(json2['data']['accountProfits'][0]['profitAmount'] ,'.8f')) + baseTotalminedList[x]}")
        json3 = (client.mining_worker_list(algo="sha256", userName=usuariosPoolList[x]))
        print(f"Cantidad de ASICs {(json3['data']['totalNum'])}")
        #pp.pprint(json3)
        hashrate = (str(float(json1['data']['dayHashRate'])/toTerahash))
        workers_active = (json1['data']['validNum'])
        workers_inactive = (json1['data']['invalidNum'])
        total_paid = float(format(json2['data']['accountProfits'][0]['profitAmount'] ,'.8f')) + baseTotalminedList[x]
        paid_yestarday = json1['data']['profitYesterday']['BTC']
        paid_today_estimate = json1['data']['profitToday']['BTC']
        paid_today_estimate = paid_yestarday

        print(f" {usuariosPoolList[x]} {hashrate}Th/s")
        print(f"Active Workers: {workers_active}")
        print(f"Offline Workers: {workers_inactive}")
        print(f"Total Paid: {total_paid}")
        print(f"Paid Yesterday: {paid_yestarday}")
        print(f"Today estimate: {paid_today_estimate}")
        update_usd(usuariosPoolList[x])
        update_btc(usuariosPoolList[x])
        update_inversionInicial(usuariosPoolList[x],inversionInicialList[x])
        zabbix_push(usuariosPoolList[x], "valor_kwh", valorKhwList[x])
        zabbix_push(usuariosPoolList[x], "hashrate", hashrate)
        zabbix_push(usuariosPoolList[x], "workers_active", workers_active)
        zabbix_push(usuariosPoolList[x], "workers_inactive", workers_inactive)
        zabbix_push(usuariosPoolList[x], "total_paid", total_paid)
        zabbix_push(usuariosPoolList[x], "paid_today_estimate", paid_today_estimate)
        zabbix_push(usuariosPoolList[x], "paid_ayer", paid_yestarday)
        zabbix_push(usuariosPoolList[x], "rev_share", revShareList[x])
        print("----------------\n\n\n")

schedule.every(5).minutes.do(job)
while True:
        schedule.run_pending()
