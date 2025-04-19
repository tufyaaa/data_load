import requests
import pandas as pd
import logging
import time
import datetime
import numpy as np
import os
import subprocess
import sys

token = 'eyJraWQiOiI3ZGRkNzk4YS1hZGQxLTRmMjMtODVmMi0yZjE0Zjc1ODM4ZTgiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhcmVhIjoibnkiLCJzY29udGV4dCI6IkNnc0lCeElIZFc1cmJtOTNiZ29vQ0FNU0pETTFOVFl3WVRKakxXRXhORGd0TkRZM05DMDVPREkxTFRNNU1qYzVaRE01T1RkbU1Rb0VDQVVTQUFvRUNBQVNBQW9FQ0FJU0FBb0ZDQWdTQVRFS0JBZ0pFZ0FLQkFnS0VnQUtLQWdFRWlRM1pHUmtOems0WVMxaFpHUXhMVFJtTWpNdE9EVm1NaTB5WmpFMFpqYzFPRE00WlRnYUN3ai9oS3UrQmhEQW42c0RJZ3NJLzdUVDBnY1F3SityQXlnQyIsInppcHBlZCI6dHJ1ZSwiY3JlYXRlZCI6IjE3NDEzNDEzMTEiLCJyZW5ld0V4cCI6IjIwNTI0Njc3MTEiLCJzZXNzIjoiSDRzSUFBQUFBQUFBLzVPYXg4akJxTVNneGNiRjRoamtIQUdpSS93aWd5RzBPNWoyZEkxd0I5Rk9qaUVRY2NkZ1Y0aThMMVNkc3krSTl2VU1CdXYzZEhNTmhab0RGbmR6ZGdxQmlQdjRRT2hRaUQ2b2VJU3pxek9FOW9XWTZ3T2tPYmpZL0FPQ0hCVVVsT0s1VkF5TmpDMHNVMU9UZGMxVERCTjFUUkxOa25RdEROSlNkVk1NTEEwTjA4eVRMY3hTTFlXNExzeS9zT0hDcGd0YkwreVdFbFM0c1BEQ0xpQm4zOFdHaTIwWDlsM1lwQ1JSVUp4ZGxsOVVVcHhmNXBDY1gxU2dsNWFabDVpclYxVGF3Y2dFQU52WWRWTUZBUUFBIiwiaXNzIjoidHhzZXJ2ZXIiLCJrZXlJZCI6IjdkZGQ3OThhLWFkZDEtNGYyMy04NWYyLTJmMTRmNzU4MzhlOCIsImZpcmViYXNlIjoiIiwic2VjcmV0cyI6IjJ0QWNMYzNJbjQ3L2JQeTBLYjdnWVE9PSIsInByb3ZpZGVyIjoiSU5URVJOQUwiLCJzY29wZSI6IkNBRVFBUSIsInRzdGVwIjoiZmFsc2UiLCJleHAiOjIwNTIzODEzMTEsImp0aSI6IjM1NTYwYTJjLWExNDgtNDY3NC05ODI1LTM5Mjc5ZDM5OTdmMSJ9.bsImy0zi3ocqMCYVbJMVsrEAu_HRaLpEn0f1mXZULcR8DSynuWu6labiuro3qwj6Ob7RcI8bbJSGJ5MfvcmeAA'

def handle_response(response):
    try:
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error: {http_err}")
    except requests.exceptions.JSONDecodeError as json_err:
        logging.error(f"JSON decode error: {json_err}")
        logging.error(f"Response content: {response.text}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    return None

def get_info(ticker, mic='XCME'):

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'eyJraWQiOiI3ZGRkNzk4YS1hZGQxLTRmMjMtODVmMi0yZjE0Zjc1ODM4ZTgiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhcmVhIjoibnkiLCJzY29udGV4dCI6IkNnc0lCeElIZFc1cmJtOTNiZ29vQ0FNU0pETTFOVFl3WVRKakxXRXhORGd0TkRZM05DMDVPREkxTFRNNU1qYzVaRE01T1RkbU1Rb0VDQVVTQUFvRUNBQVNBQW9FQ0FJU0FBb0ZDQWdTQVRFS0JBZ0pFZ0FLQkFnS0VnQUtLQWdFRWlRM1pHUmtOems0WVMxaFpHUXhMVFJtTWpNdE9EVm1NaTB5WmpFMFpqYzFPRE00WlRnYUN3ai9oS3UrQmhEQW42c0RJZ3NJLzdUVDBnY1F3SityQXlnQyIsInppcHBlZCI6dHJ1ZSwiY3JlYXRlZCI6IjE3NDEzNDEzMTEiLCJyZW5ld0V4cCI6IjIwNTI0Njc3MTEiLCJzZXNzIjoiSDRzSUFBQUFBQUFBLzVPYXg4akJxTVNneGNiRjRoamtIQUdpSS93aWd5RzBPNWoyZEkxd0I5Rk9qaUVRY2NkZ1Y0aThMMVNkc3krSTl2VU1CdXYzZEhNTmhab0RGbmR6ZGdxQmlQdjRRT2hRaUQ2b2VJU3pxek9FOW9XWTZ3T2tPYmpZL0FPQ0hCVVVsT0s1VkF5TmpDMHNVMU9UZGMxVERCTjFUUkxOa25RdEROSlNkVk1NTEEwTjA4eVRMY3hTTFlXNExzeS9zT0hDcGd0YkwreVdFbFM0c1BEQ0xpQm4zOFdHaTIwWDlsM1lwQ1JSVUp4ZGxsOVVVcHhmNXBDY1gxU2dsNWFabDVpclYxVGF3Y2dFQU52WWRWTUZBUUFBIiwiaXNzIjoidHhzZXJ2ZXIiLCJrZXlJZCI6IjdkZGQ3OThhLWFkZDEtNGYyMy04NWYyLTJmMTRmNzU4MzhlOCIsImZpcmViYXNlIjoiIiwic2VjcmV0cyI6IjJ0QWNMYzNJbjQ3L2JQeTBLYjdnWVE9PSIsInByb3ZpZGVyIjoiSU5URVJOQUwiLCJzY29wZSI6IkNBRVFBUSIsInRzdGVwIjoiZmFsc2UiLCJleHAiOjIwNTIzODEzMTEsImp0aSI6IjM1NTYwYTJjLWExNDgtNDY3NC05ODI1LTM5Mjc5ZDM5OTdmMSJ9.bsImy0zi3ocqMCYVbJMVsrEAu_HRaLpEn0f1mXZULcR8DSynuWu6labiuro3qwj6Ob7RcI8bbJSGJ5MfvcmeAA',
    }

    json_data = {
        "history_requests": [
            {
                "identifier": {
                    "ticker_mic": {
                        "ticker": ticker,
                        "mic": mic
                    }
                },
            }
        ]
    }
    response = requests.put(
        'https://ftrr01.finam.ru/grpc-json/reference/v1/securities_history_request',
        headers=headers,
        json=json_data,
    ) 

    dict_data = handle_response(response)

    # with open(r'C:\Users\rkalmetev-gph\Downloads\json1.json', 'w') as fp:
    #     json.dump(dict_data, fp, indent=6)
    try:
        instr_id = dict_data[0]['securities'][0]['security']['security']['common']['securityId']

        trade_last_day = dict_data[0]['securities'][0]['security']['security']['future']['tradeLastDay']
        last_timestamp = datetime.datetime(trade_last_day['year'], trade_last_day['month'], trade_last_day['day'],
                                        tzinfo = datetime.timezone.utc).isoformat().replace('+00:00', 'Z')
        return instr_id, last_timestamp
    except (KeyError, IndexError, TypeError) as e:
        logging.error(f"Data structure error: {str(e)}")
        return 0, 0
    


log_file_path = os.path.join('C:', os.sep, 'Users', 'etsvetkov', 'algotrading', 'application.log')
# Настройка логирования
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    # handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# underlyings = ['SR3', 'ES', 'MNQ', 'MES', 'NQ', 'RTY', 'SR1', '6J', '6E', '6A', '6B', 'M2K', '6C', '6M', 'MBT', 'LE', '6N', 'M6E', 'HE', '6S', 'NIY', 'MET', 'M6A', '6L', 'ASR', 'GF', 'EMD', 'SDA', 'NKD', 'BTC', 'MNI', 'E7', 'ETH', '6Z', 'RP', 'M6B', 'MCD', 'BFF', 'ESR', 'EUS', 'SIR', 'XAE', 'XAU', 'XAP', 'XAI', 'RY', 'RF', 'MNK', 'RSG', 'XAF', 'MSF', 'CNH', 'J7', 'XAV', 'KRW', 'XAY', 'TBF3', 'LBR', 'XAB', 'DC', 'MJY', 'XAK', 'SDI', 'NOK', 'MSL', 'RSV', 'SEK', 'ESG', 'EWF', 'RDA', 'EAD', 'GIE', 'CB', 'PJY', 'XAR', 'XAZ', 'MMC', 'CSC', 'ECD', 'CPO', 'AJY', 'GNF', 'SOL', 'RS1', 'G2K', 'PLN', 'SXI', 'ESK', 'SXR', 'SXT']
# underlyings = ['NQ', 'ES']
underlyings = ['M6E', 'M6B', 'M6A', 'M2K', 'E7', '6S', '6N', '6M', '6L', '6J', '6E', '6C', '6B','6A']
contract_expirations_month = ['H','M','U','Z']
years = np.arange(17,25).astype(str)
tickers = ['M17','U17','Z17','H18','M18','U18','Z18','H19','M19','U9','Z9','H0','M0','U0','Z0',
           'H1','M1','U1','Z1','H2','M2','U2','Z2','H3','M3','U3','Z23','H24','M24','U24','Z24']

for underlying in underlyings:
    logger.info(f"Ticker - {underlying}")
    logger.info("Processing Method 1...")
    zero_count_method1 = 0
    with open('tickers_info1.txt', 'w') as f:
        f.write("")

    for ticker in tickers:
        tic = underlying + ticker
        try:
            logger.debug(f"Processing {tic}")
            id, disabled_timestamp = get_info(tic, 'XCME')
            
            if id == 0 and disabled_timestamp == 0:
                zero_count_method1 += 1
                logger.warning(f"Zero values found for {tic}")
            
            with open('tickers_info1.txt', 'a') as f:  
                f.write(f"{tic}\n") 
                f.write(f"{id}, {disabled_timestamp}\n")
                
        except Exception as e:
            logger.error(f"Error processing {tic}: {str(e)}")
            continue

    logger.info(f"Method 1 complete. Zero entries: {zero_count_method1}")

    # Метод 2
    logger.info("Processing Method 2...")
    zero_count_method2 = 0
    with open('tickers_info2.txt', 'w') as f:
        f.write("")  

    for year in years:
        for exp in contract_expirations_month:
            ticker = underlying + exp + year
            try:
                logger.debug(f"Processing {ticker}")
                id, disabled_timestamp = get_info(ticker, 'XCME')
                
                if id == 0 and disabled_timestamp == 0:
                    zero_count_method2 += 1
                    logger.warning(f"Zero values found for {ticker}")
                    
                with open('tickers_info2.txt', 'a') as f:  
                    f.write(f"{ticker}\n") 
                    f.write(f"{id}, {disabled_timestamp}\n")
                    
            except Exception as e:
                logger.error(f"Error processing {ticker}: {str(e)}")
                continue

    logger.info(f"Method 2 complete. Zero entries: {zero_count_method2}")

    # Выбор лучшего метода
    if zero_count_method1 < zero_count_method2:
        source_file = 'tickers_info1.txt'
        zero_count = zero_count_method1
        logger.info("Method 1 selected as better")
    else:
        source_file = 'tickers_info2.txt'
        zero_count = zero_count_method2
        logger.info("Method 2 selected as better")

    # Запись результата и удаление временных файлов
    try:
        with open(source_file, 'r') as src, open('ticker_info.txt', 'w') as dst:
            dst.write(src.read())
        logger.info(f"Final data saved to ticker_info.txt")
        
    except IOError as e:
        logger.error(f"File operation failed: {str(e)}")
    finally:
        # Удаление временных файлов
        temp_files = ['tickers_info1.txt', 'tickers_info2.txt']
        for file in temp_files:
            try:
                os.remove(file)
                logger.info(f"Temporary file {file} deleted")
            except OSError as e:
                logger.warning(f"Error deleting {file}: {str(e)}")

    logger.info(f"Processing complete. Total zero entries: {zero_count}")

    logger.info("Launching data_loader.py...")
    try:
        subprocess.run([sys.executable, "data_load\\data_loader.py"], check=True)
        logger.info("data_loader.py executed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing data_loader.py: {str(e)}")
        continue  # Прерываем обработку текущего underlying при ошибке

    # Формируем аргументы для merger.py
    folder_path = os.path.join('C:', os.sep, 'Users', 'etsvetkov', 'algotrading', 'algo','data', underlying)
    output_filename = f"data\\{underlying}.csv"
    
    # Запускаем merger.py с аргументами
    logger.info("Launching merger.py...")
    try:
        subprocess.run([
            sys.executable, 
            "data_load\\merger.py",
            "--folder", folder_path,
            "--underlying", underlying,
            "--output", output_filename
        ], check=True)
        logger.info(f"merger.py executed successfully. Output: {output_filename}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing merger.py: {str(e)}")



