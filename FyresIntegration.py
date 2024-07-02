from fyers_apiv3 import fyersModel
import webbrowser
from datetime import datetime, timedelta, date
from time import sleep
import os
import pyotp
import requests
import json
import math
import pytz
from urllib.parse import parse_qs, urlparse
import warnings
import pandas as pd



fyers=None

def apiactivation(client_id,redirect_uri,response_type,state,secret_key,grant_type):
    appSession = fyersModel.SessionModel(client_id = client_id, redirect_uri = redirect_uri,response_type=response_type,state=state,secret_key=secret_key,grant_type=grant_type)
    # ## Make  a request to generate_authcode object this will return a login url which you need to open in your browser from where you can get the generated auth_code
    generateTokenUrl = appSession.generate_authcode()
    print("generateTokenUrl: ",generateTokenUrl)

def automated_login(client_id,secret_key,FY_ID,TOTP_KEY,PIN,redirect_uri):

    pd.set_option('display.max_columns', None)
    warnings.filterwarnings('ignore')

    import base64


    def getEncodedString(string):
        string = str(string)
        base64_bytes = base64.b64encode(string.encode("ascii"))
        return base64_bytes.decode("ascii")

    global fyers

    URL_SEND_LOGIN_OTP = "https://api-t2.fyers.in/vagator/v2/send_login_otp_v2"
    res = requests.post(url=URL_SEND_LOGIN_OTP, json={"fy_id": getEncodedString(FY_ID), "app_id": "2"}).json()
    print(res)

    if datetime.now().second % 30 > 27: sleep(5)
    URL_VERIFY_OTP = "https://api-t2.fyers.in/vagator/v2/verify_otp"
    res2 = requests.post(url=URL_VERIFY_OTP,
                         json={"request_key": res["request_key"], "otp": pyotp.TOTP(TOTP_KEY).now()}).json()
    print(res2)

    ses = requests.Session()
    URL_VERIFY_OTP2 = "https://api-t2.fyers.in/vagator/v2/verify_pin_v2"
    payload2 = {"request_key": res2["request_key"], "identity_type": "pin", "identifier": getEncodedString(PIN)}
    res3 = ses.post(url=URL_VERIFY_OTP2, json=payload2).json()
    print(res3)

    ses.headers.update({
        'authorization': f"Bearer {res3['data']['access_token']}"
    })

    TOKENURL = "https://api-t1.fyers.in/api/v3/token"
    payload3 = {"fyers_id": FY_ID,
                "app_id": client_id[:-4],
                "redirect_uri": redirect_uri,
                "appType": "100", "code_challenge": "",
                "state": "None", "scope": "", "nonce": "", "response_type": "code", "create_cookie": True}

    res3 = ses.post(url=TOKENURL, json=payload3).json()
    url = res3['Url']
    parsed = urlparse(url)
    auth_code = parse_qs(parsed.query)['auth_code'][0]
    grant_type = "authorization_code"

    response_type = "code"

    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type=response_type,
        grant_type=grant_type
    )
    session.set_token(auth_code)
    response = session.generate_token()
    access_token = response['access_token']
    fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path=os.getcwd())
    print(fyers.get_profile())

def get_ltp(SYMBOL):
    global fyers
    data={"symbols":f"{SYMBOL}"}
    res=fyers.quotes(data)
    if 'd' in res and len(res['d']) > 0:
        lp = res['d'][0]['v']['lp']
        return lp

    else:
        print("Last Price (lp) not found in the response.")




def get_position():
    global fyers
      ## This will provide all the trade related information
    res=fyers.positions()
    return res

def get_orderbook():
    global fyers
    res = fyers.orderbook()
    return res
      ## This will provide the user with all the order realted information

def get_tradebook():
    global fyers
    res = fyers.tradebook()
    return res


def fetchOHLC_Scanner(symbol):
    dat =str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(5)).date())
    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat ,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)
    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)
    df['date']=df['date'].apply(pd.Timestamp,unit='s',tzinfo=pytz.timezone('Asia/Kolkata'))
    return df.tail(5)

def fetchOHLC(symbol):
    dat =str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(17)).date())
    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat ,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)
    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)
    df['date']=df['date'].apply(pd.Timestamp,unit='s',tzinfo=pytz.timezone('Asia/Kolkata'))
    return df.tail(10)


def fetchOHLC_get_selected_price(symbol, date):
    dat = str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(25)).date())
    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)
    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)
    df['date'] = pd.to_datetime(df['date'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata').dt.date
    target_date = pd.to_datetime(date).date()
    matching_row = df[df['date'] == target_date]
    if matching_row.empty:
        return 0
    else:
        close_price = matching_row.iloc[0]['close']
        return close_price

