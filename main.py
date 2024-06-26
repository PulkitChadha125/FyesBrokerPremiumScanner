import threading
import requests
import FyresIntegration
import time
import traceback
from datetime import datetime, timedelta
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, render_template
lock = threading.Lock()
import os
import sys
import traceback
if getattr(sys, 'frozen', False):
    current_dir = os.path.dirname(sys.executable)
else:
    current_dir = os.path.dirname(os.path.abspath(__file__))

template_folder = os.path.join(current_dir, 'templates')
print(template_folder)

app = Flask(__name__, template_folder=template_folder)
def get_user_settings():
    global result_dict
    try:
        csv_path = 'MasterFile.csv'
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()
        result_dict = {}
        for index, row in df.iterrows():
            symbol_dict = {
                'SYMBOL': row['SYMBOL'],
                'EXPIERY': row['EXPIERY'],
            }
            result_dict[row['SYMBOL']] = symbol_dict
        print("result_dict: ", result_dict)
    except Exception as e:
        print("Error happened in fetching symbol", str(e))

get_user_settings()

def get_api_credentials():
    credentials = {}
    
    try:
        df = pd.read_csv('Credentials.csv')
        for index, row in df.iterrows():
            title = row['Title']
            value = row['Value']
            credentials[title] = value
    except pd.errors.EmptyDataError:
        print("The CSV file is empty or has no data.")
    except FileNotFoundError:
        print("The CSV file was not found.")
    except Exception as e:
        print("An error occurred while reading the CSV file:", str(e))

    return credentials

credentials_dict = get_api_credentials()
redirect_uri=credentials_dict.get('redirect_uri')
client_id=credentials_dict.get('client_id')
secret_key=credentials_dict.get('secret_key')
grant_type=credentials_dict.get('grant_type')
response_type=credentials_dict.get('response_type')
state=credentials_dict.get('state')
TOTP_KEY=credentials_dict.get('totpkey')
FY_ID=credentials_dict.get('FY_ID')
PIN=credentials_dict.get('PIN')

FyresIntegration.automated_login(client_id=client_id, redirect_uri=redirect_uri, secret_key=secret_key, FY_ID=FY_ID,
                                     PIN=PIN, TOTP_KEY=TOTP_KEY)

def symbols():
    url = "https://public.fyers.in/sym_details/NSE_FO_sym_master.json"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame.from_dict(data, orient='index')
    column_mapping = {
        "lastUpdate": "lastUpdate","exSymbol": "exSymbol","qtyMultiplier": "qtyMultiplier","previousClose": "previousClose",
        "exchange": "exchange","exSeries": "exSeries","optType": "optType","mtf_margin": "mtf_margin","is_mtf_tradable": "is_mtf_tradable",
        "exSymName": "exSymName","symTicker": "symTicker","exInstType": "exInstType","fyToken": "fyToken","upperPrice": "upperPrice",
        "lowerPrice": "lowerPrice","segment": "segment","symbolDesc": "symbolDesc","symDetails": "symDetails","exToken": "exToken",
        "strikePrice": "strikePrice","minLotSize": "minLotSize","underFyTok": "underFyTok","currencyCode": "currencyCode","underSym": "underSym","expiryDate": "expiryDate",
        "tradingSession": "tradingSession","asmGsmVal": "asmGsmVal","faceValue": "faceValue","tickSize": "tickSize","exchangeName": "exchangeName",
        "originalExpDate": "originalExpDate","isin": "isin","tradeStatus": "tradeStatus","qtyFreeze": "qtyFreeze","previousOi": "previousOi"
    }
    df.rename(columns=column_mapping, inplace=True)
    for col in column_mapping.values():
        if col not in df.columns:
            df[col] = None
    csv_file = 'Master.csv'
    df.to_csv(csv_file, index=False)
    print(f'Fno data has been successfully written to {csv_file}')

symbols()




def ATM_CE_AND_PE_COMBIMED(ltp,symbol,exp):
    try:
        monthlyexp = exp
        date_obj = datetime.strptime(monthlyexp, "%d-%b-%y")
        formatted_date = date_obj.strftime("%Y-%m-%d")
        monthlyexp = formatted_date
        monthlyexp = datetime.strptime(monthlyexp, "%Y-%m-%d").date()
        pf = pd.read_csv("Master.csv")
        pf['expiryDate'] = pd.to_datetime(pf['expiryDate'], unit='s').dt.date
        filtered_df = pf[(pf['expiryDate'] == monthlyexp) & (pf['optType'] == 'CE') & (pf['exSymbol'] == symbol)]
        if not filtered_df.empty:
            filtered_df["strike_diff"] = abs(
                filtered_df["strikePrice"] - ltp)
            min_diff_row = filtered_df.loc[filtered_df['strike_diff'].idxmin()]
        strike=int(min_diff_row["strikePrice"])
        lots=int(min_diff_row["minLotSize"])
        cesymname=min_diff_row["symTicker"]
        pesymname = cesymname.rsplit('CE', 1)[0] + 'PE'
        ce_ltp=FyresIntegration.get_ltp(cesymname)
        pe_ltp=FyresIntegration.get_ltp(pesymname)
        com=ce_ltp+pe_ltp
        return int(com),lots
    except Exception as e:
        print(f"ATM_CE_AND_PE_COMBIMED error : {str(e)}")

def PREMIUM_COLLECTED(lots,combinedpremium):

    return float(lots*combinedpremium)
def calculate_xpercent(ltp, combinedpremium):
    xpercent = (combinedpremium / ltp) * 100
    return xpercent

data_rows=[]
def main_strategy():
    global result_dict
    try:
        for symbol, params in result_dict.items():
            symbol_value = params['SYMBOL']
            timestamp = datetime.now()
            timestamp = timestamp.strftime("%d/%m/%Y %H:%M:%S")
            if isinstance(symbol_value, str):
                date_object = datetime.strptime(params['EXPIERY'], '%d-%b-%y')
                new_date_string = date_object.strftime('%y%b').upper()
                formatedsymbol = f"NSE:{params['SYMBOL']}{new_date_string}FUT"
                data=FyresIntegration.fetchOHLC_Scanner(formatedsymbol)
                df = pd.DataFrame(data)
                first_row = df.iloc[0]
                second_row = df.iloc[1]
                close= second_row['close']
                com, lots=ATM_CE_AND_PE_COMBIMED(ltp=close, symbol=symbol_value, exp=params['EXPIERY'])
                combined_pnl = com
                percentof = calculate_xpercent(ltp=close, combinedpremium=combined_pnl)
                data_rows.append({'Symbol': symbol_value,'LTP': close, 'CombinedPremium': combined_pnl,
                                  'PERCENTAGEOF_LTP': percentof, 'PREMIUM_COLLECTED': PREMIUM_COLLECTED(lots,combined_pnl)})

        result_df = pd.DataFrame(data_rows)
        result_df.to_csv("webdata.csv", index=False)


    except Exception as e:
        print("Error happened in Main strategy loop: ", str(e))
        traceback.print_exc()

@app.route('/')
def index():
    try:
        df = pd.read_csv("webdata.csv")
        df = df.drop(columns=['Unnamed: 0', 'Unnamed: 0.1','NFO Trading Symbol'], errors='ignore')
        html_table = df.to_html(index=False, classes='sortable')  # Add 'sortable' class

    except Exception as e:
        print(f"Error happened in rendering: {str(e)}")
        html_table = "<p>Error occurred while rendering the table.</p>"

    return render_template('index.html', html_table=html_table)

if __name__ == '__main__':
    main_strategy()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scheduler = BackgroundScheduler()
    scheduler.add_job(main_strategy, 'interval', minutes=2)
    scheduler.start()
    app.run()