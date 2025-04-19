import requests
import pandas as pd
import logging
import time
import datetime
import numpy as np
from requests_toolbelt.utils import dump
import os
import re

token = 'eyJraWQiOiI3ZGRkNzk4YS1hZGQxLTRmMjMtODVmMi0yZjE0Zjc1ODM4ZTgiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhcmVhIjoibnkiLCJzY29udGV4dCI6IkNnc0lCeElIZFc1cmJtOTNiZ29vQ0FNU0pETTFOVFl3WVRKakxXRXhORGd0TkRZM05DMDVPREkxTFRNNU1qYzVaRE01T1RkbU1Rb0VDQVVTQUFvRUNBQVNBQW9FQ0FJU0FBb0ZDQWdTQVRFS0JBZ0pFZ0FLQkFnS0VnQUtLQWdFRWlRM1pHUmtOems0WVMxaFpHUXhMVFJtTWpNdE9EVm1NaTB5WmpFMFpqYzFPRE00WlRnYUN3ai9oS3UrQmhEQW42c0RJZ3NJLzdUVDBnY1F3SityQXlnQyIsInppcHBlZCI6dHJ1ZSwiY3JlYXRlZCI6IjE3NDEzNDEzMTEiLCJyZW5ld0V4cCI6IjIwNTI0Njc3MTEiLCJzZXNzIjoiSDRzSUFBQUFBQUFBLzVPYXg4akJxTVNneGNiRjRoamtIQUdpSS93aWd5RzBPNWoyZEkxd0I5Rk9qaUVRY2NkZ1Y0aThMMVNkc3krSTl2VU1CdXYzZEhNTmhab0RGbmR6ZGdxQmlQdjRRT2hRaUQ2b2VJU3pxek9FOW9XWTZ3T2tPYmpZL0FPQ0hCVVVsT0s1VkF5TmpDMHNVMU9UZGMxVERCTjFUUkxOa25RdEROSlNkVk1NTEEwTjA4eVRMY3hTTFlXNExzeS9zT0hDcGd0YkwreVdFbFM0c1BEQ0xpQm4zOFdHaTIwWDlsM1lwQ1JSVUp4ZGxsOVVVcHhmNXBDY1gxU2dsNWFabDVpclYxVGF3Y2dFQU52WWRWTUZBUUFBIiwiaXNzIjoidHhzZXJ2ZXIiLCJrZXlJZCI6IjdkZGQ3OThhLWFkZDEtNGYyMy04NWYyLTJmMTRmNzU4MzhlOCIsImZpcmViYXNlIjoiIiwic2VjcmV0cyI6IjJ0QWNMYzNJbjQ3L2JQeTBLYjdnWVE9PSIsInByb3ZpZGVyIjoiSU5URVJOQUwiLCJzY29wZSI6IkNBRVFBUSIsInRzdGVwIjoiZmFsc2UiLCJleHAiOjIwNTIzODEzMTEsImp0aSI6IjM1NTYwYTJjLWExNDgtNDY3NC05ODI1LTM5Mjc5ZDM5OTdmMSJ9.bsImy0zi3ocqMCYVbJMVsrEAu_HRaLpEn0f1mXZULcR8DSynuWu6labiuro3qwj6Ob7RcI8bbJSGJ5MfvcmeAA'
log_file_path = os.path.join('C:', os.sep, 'Users', 'etsvetkov', 'algotrading', 'algo','application.log')
# Настройка логирования
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    # handlers=[logging.StreamHandler()]
)

def get_base_ticker(full_ticker):
    if len(full_ticker) >= 2 and full_ticker[-1].isdigit() and full_ticker[-2].isdigit():
        return full_ticker[:-3]
    elif len(full_ticker) >= 1 and full_ticker[-1].isdigit():
        # Удаляем 2 символа с конца
        return full_ticker[:-2]
    return full_ticker

def parse_contracts_file(filename):
    """Парсинг файла с контрактами"""
    contracts = []
    with open(filename, 'r') as f:
        lines = f.readlines()
    
    for i in range(0, len(lines), 2):
        if i+1 >= len(lines):
            break
            
        ticker = lines[i].strip()
        data_line = lines[i+1].strip()
        
        if data_line == '0, 0':
            logging.warning(f"Skipping {ticker} - invalid data")
            continue
            
        try:
            security_id, expiry_date = data_line.split(', ')
            contracts.append({
                'ticker': ticker,
                'security_id': int(security_id),
                'expiry_date': datetime.datetime.fromisoformat(expiry_date.replace('Z', ''))
            })
        except Exception as e:
            logging.error(f"Error parsing line {i+1}: {e}")
    
    return contracts
    
def format_ticker(ticker, date_str):
    if len(ticker) == 0:
        return ticker
    
    # Извлекаем последние 2 цифры года из даты
    year_code = date_str[2:4]
    
    # Определяем сколько цифр нужно обрезать
    digits_to_trim = 0
    if len(ticker) >= 2 and ticker[-2:].isdigit():
        digits_to_trim = 2
    elif ticker[-1].isdigit():
        digits_to_trim = 1
    
    # Обрезаем и добавляем годовой код
    return ticker[:-digits_to_trim] + year_code if digits_to_trim else ticker + year_code

    
    return ticker

def get_1min_candles(security_id, start, to):
    """Получение минутных свечей"""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': token
    }

    json_data = {
        'id': {'securityId': security_id},
        'timeFrame': {'timeUnit': 'MINUTE', 'count': 1},
        'period': {"start": start, "to": to}
    }

    try:
        response = requests.get(
            'https://ftrr01.finam.ru/grpc-json/ta/v1/get_intraday_candles',
            headers=headers,
            json=json_data,
            timeout=15
        )
        data = response.json()
        
        if 'candles' not in data:
            logging.warning(f"No candles found for {security_id} {start}-{to}")
            return pd.DataFrame()
            
        candles = []
        for candle in data['candles']:
            try:
                ts = candle['timestamp']
                open_ = float(candle['open']['num']) * 10 ** -int(candle['open']['scale'])
                high = float(candle['high']['num']) * 10 ** -int(candle['high']['scale'])
                low = float(candle['low']['num']) * 10 ** -int(candle['low']['scale'])
                close = float(candle['close']['num']) * 10 ** -int(candle['close']['scale'])
                volume = float(candle['volume']['num']) * 10 ** -int(candle['volume']['scale'])
                
                candles.append([ts, open_, high, low, close, volume])
            except KeyError as e:
                logging.warning(f"Missing key in candle data: {e}")
        
        return pd.DataFrame(
            candles,
            columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
        )
        
    except Exception as e:
        logging.error(f"Attempt failed: {str(e)}")
    
    return pd.DataFrame()

def download_history(contract, days_before=150):
    """Загрузка исторических данных для контракта от начала к концу"""
    end_date = contract['expiry_date']
    start_date = end_date - datetime.timedelta(days=days_before)
    
    all_candles = pd.DataFrame()
    current_start = start_date
    chunk_size = datetime.timedelta(days=2)  # Размер чанка в днях
    
    while current_start < end_date:
        # Определяем конец текущего интервала
        current_end = min(current_start + chunk_size, end_date)
        
        # Форматируем даты для запроса
        start_str = current_start.isoformat() + 'Z'
        end_str = current_end.isoformat() + 'Z'
        
        logging.info(f"Downloading {contract['ticker']} {start_str} - {end_str}")
        
        # Получаем данные
        df = get_1min_candles(
            contract['security_id'],
            start_str,
            end_str
        )
        
        if not df.empty:
            all_candles = pd.concat([all_candles, df])
        
        current_start = current_end
    
    
    # Сохранение результатов
    if not all_candles.empty:
        dir_path = f'data/{get_base_ticker(contract["ticker"])}'
        os.makedirs(dir_path, exist_ok=True)
        file_path = f'{dir_path}/{format_ticker(contract["ticker"], str(contract['expiry_date']))}.csv'
        all_candles.to_csv(file_path, sep=';', index=False)
        logging.info(f"Saved {len(all_candles)} rows to {file_path}")
    else:
        logging.warning(f"No data found for {contract['ticker']}")



contracts = parse_contracts_file('ticker_info.txt')
for contract in contracts:
    download_history(contract)