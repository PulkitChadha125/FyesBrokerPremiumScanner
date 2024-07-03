import threading
import requests
import FyresIntegration
import time
import traceback
from datetime import datetime, timedelta
import os
import sys
import traceback
import pandas as pd

def data_formating():
    gf = pd.read_csv('premium_combined_pivoted_data.csv')
    gf = gf.set_index('Date').T
    gf = gf.sort_index(ascending=False)
    gf = gf.reset_index()
    gf = gf[gf.columns[::-1]]
    gf = gf.rename(columns={'index': 'Symbol'})
    today_date = datetime.today().date()
    formatted_today_date = today_date.strftime('%Y-%m-%d')
    formatted_today_date_string = str(formatted_today_date)
    yesterday_date = today_date - timedelta(days=1)
    formatted_yesterday_date = yesterday_date.strftime('%Y-%m-%d')
    formatted_yesterday_date_string = str(formatted_yesterday_date)
    if formatted_today_date_string in gf.columns:
        gf = gf.drop(columns=formatted_today_date_string)
    if formatted_yesterday_date_string in gf.columns:
        gf = gf.drop(columns=formatted_yesterday_date_string)
    gf.to_csv("premium_combined_pivoted_data.csv", index=False)
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





def symbols():
    url = "https://public.fyers.in/sym_details/NSE_FO_sym_master.json"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame.from_dict(data, orient='index')
    column_mapping = {
        "lastUpdate": "lastUpdate", "exSymbol": "exSymbol", "qtyMultiplier": "qtyMultiplier",
        "previousClose": "previousClose",
        "exchange": "exchange", "exSeries": "exSeries", "optType": "optType", "mtf_margin": "mtf_margin",
        "is_mtf_tradable": "is_mtf_tradable",
        "exSymName": "exSymName", "symTicker": "symTicker", "exInstType": "exInstType", "fyToken": "fyToken",
        "upperPrice": "upperPrice",
        "lowerPrice": "lowerPrice", "segment": "segment", "symbolDesc": "symbolDesc", "symDetails": "symDetails",
        "exToken": "exToken",
        "strikePrice": "strikePrice", "minLotSize": "minLotSize", "underFyTok": "underFyTok",
        "currencyCode": "currencyCode", "underSym": "underSym", "expiryDate": "expiryDate",
        "tradingSession": "tradingSession", "asmGsmVal": "asmGsmVal", "faceValue": "faceValue", "tickSize": "tickSize",
        "exchangeName": "exchangeName",
        "originalExpDate": "originalExpDate", "isin": "isin", "tradeStatus": "tradeStatus", "qtyFreeze": "qtyFreeze",
        "previousOi": "previousOi",
        "fetchHistory": None
    }
    df.rename(columns=column_mapping, inplace=True)
    for col in column_mapping.values():
        if col not in df.columns:
            df[col] = None
    csv_file = 'Master.csv'
    df.to_csv(csv_file, index=False)
    print(f'Fno data has been successfully written to {csv_file}')

def ATM_CE_AND_PE_COMBIMED_10day_ver(close_price,symbol,exp):
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
                filtered_df["strikePrice"] - close_price)
            min_diff_row = filtered_df.loc[filtered_df['strike_diff'].idxmin()]
        strike=int(min_diff_row["strikePrice"])
        lots=int(min_diff_row["minLotSize"])
        cesymname=min_diff_row["symTicker"]
        pesymname = cesymname.rsplit('CE', 1)[0] + 'PE'
        return cesymname,pesymname
    except Exception as e:
        print(f"ATM_CE_AND_PE_COMBIMED_10day_ver  : {str(e)}")


def fetch_history():
    global result_dict
    trading_symbols_dict = {}
    try:
        for symbol, params in result_dict.items():
            symbol_value = params['SYMBOL']
            timestamp = datetime.now()
            timestamp = timestamp.strftime("%d/%m/%Y %H:%M:%S")

            if isinstance(symbol_value, str):
                date_object = datetime.strptime(params['EXPIERY'], '%d-%b-%y')
                new_date_string = date_object.strftime('%y%b').upper()
                formatedsymbol = f"NSE:{params['SYMBOL']}{new_date_string}FUT"
                print(f"{timestamp} fetch history {formatedsymbol}.. ")
                data = FyresIntegration.fetchOHLC(formatedsymbol)
                data['date'] = pd.to_datetime(data['date']).dt.strftime('%Y-%m-%d')
                date_values = data['date'][:10]
                close_prices = data['close'][:10]
                info = {}
                for idx in range(10):
                    try:
                        if idx < len(date_values) and idx < len(close_prices):
                            date_value = date_values.iloc[idx]
                            close_price = close_prices.iloc[idx]
                            cesymname, pesymname = ATM_CE_AND_PE_COMBIMED_10day_ver(close_price,symbol,exp=params['EXPIERY'])
                            ce_close_price = FyresIntegration.fetchOHLC_get_selected_price(
                                symbol=cesymname,date=date_value)
                            pe_close_price = FyresIntegration.fetchOHLC_get_selected_price(
                                symbol=pesymname,date=date_value)


                            if ce_close_price is not None and pe_close_price is not None:
                                premium_combined = ce_close_price + pe_close_price
                            else:
                                premium_combined = None


                            info[date_value] = {

                                "cecloseprice": ce_close_price,
                                "peprice": pe_close_price,
                                "premium_combined": premium_combined
                            }
                    except IndexError:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        print("*** Traceback ***")
                        traceback.print_tb(exc_traceback)
                        info[date_value] = {"premium_combined": "NA"}
                        continue
                        # Store data in 'trading_symbols_dict'
                trading_symbols_dict[symbol] = {
                            "symbol": params['SYMBOL'],
                            "info": info,
                        }
        columns = list(trading_symbols_dict.keys())
        date_columns = list(trading_symbols_dict[columns[0]]["info"].keys())
        # Create a DataFrame from 'data' with 'columns' and 'date_columns'
        data = []

        for symbol in columns:
            row = []
            for date in date_columns:
                try:
                    premium_combined = trading_symbols_dict[symbol]["info"][date]["premium_combined"]
                except KeyError:
                    premium_combined = None
                row.append(premium_combined)
            data.append(row)

        df = pd.DataFrame(data, columns=date_columns, index=columns)

        df = df.T.reset_index()
        df.columns.name = None
        df = df.rename(columns={'index': 'Date'})

        df.to_csv("premium_combined_pivoted_data.csv", index=False)
        data_formating()


    except Exception as e:
        print(f"fetch history error : {str(e)}")

get_user_settings()
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
symbols()
fetch_history()