# %%
import os
import glob
import logging
import argparse
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# %%
# Настройка логирования


def setup_logging():
    log_file_path = os.path.join('C:', os.sep, 'Users', 'etsvetkov', 'algotrading', 'application.log')
    # Настройка логирования
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        # handlers=[logging.StreamHandler()]
    )   
    logging.info("Логирование запущено.")

# Функция для преобразования буквенного кода месяца в номер месяца


def month_code_to_number(code):
    month_codes = {
        'F': 1,  # Январь
        'G': 2,  # Февраль
        'H': 3,  # Март
        'J': 4,  # Апрель
        'K': 5,  # Май
        'M': 6,  # Июнь
        'N': 7,  # Июль
        'Q': 8,  # Август
        'U': 9,  # Сентябрь
        'V': 10,  # Октябрь
        'X': 11,  # Ноябрь
        'Z': 12  # Декабрь
    }
    result = month_codes.get(code.upper())
    if result is None:
        logging.warning(f"Неизвестный код месяца: {code}")
    return result

# Извлечение информации о контракте из имени файла.
# Ожидается, что имя файла имеет формат: <UNDERLYING><MONTH_CODE><YEAR>.csv (например, NQZ4)


def parse_contract_filename(filepath, underlying):
    filename = os.path.basename(filepath)
    name, ext = os.path.splitext(filename)
    if not name.upper().startswith(underlying.upper()):
        logging.info(
            f"Файл {filename} не соответствует базовому активу {underlying}. Пропускаем.")
        return None
    try:
        # После базового актива должна идти 1 буква для месяца и 1-2 цифры для года
        month_code = name[len(underlying)]
        year_digits = name[len(underlying)+1:]
        year = int(year_digits)
        if year < 100:
            year += 2000
        month = month_code_to_number(month_code)
        if month is None:
            return None
        # Дата истечения определена как первое число месяца
        expiration = datetime(year, month, 1)
        logging.info(
            f"Файл {filename}: базовый актив={underlying}, месяц={month_code} ({month}), год={year}, истечение={expiration.date()}.")
        return {
            'filepath': filepath,
            'month_code': month_code,
            'year': year,
            'expiration': expiration
        }
    except Exception as e:
        logging.error(f"Ошибка при разборе имени файла {filename}: {e}")
        return None

# Загрузка CSV-файла и добавление информации о контракте в DataFrame


def load_contract_data(contract_info):
    filepath = contract_info['filepath']
    try:
        df = pd.read_csv(filepath, sep=';')
        df['timestamp'] = pd.to_datetime(
            df['timestamp'], format="%Y-%m-%dT%H:%M:%SZ")
        df = df.set_index('timestamp').sort_index()
        df['contract'] = os.path.basename(filepath)
        df['expiration'] = contract_info['expiration']
        logging.info(f"Загружен файл {filepath} с {len(df)} записями.")
        return df
    except Exception as e:
        logging.error(f"Ошибка при загрузке файла {filepath}: {e}")
        return None

# Определение дат rollover между контрактами


def determine_rollover_dates(dfs, volume_threshold=0.5):
    rollover_dates = []
    for i in range(len(dfs)-1):
        try:
            current_df = dfs[i]
            next_df = dfs[i+1]
            # Объединяем по дате (оставляем только общие дни)
            merged = current_df[['volume']].join(
                next_df[['volume']], lsuffix='_current', rsuffix='_next', how='inner')
            if merged.empty:
                logging.warning("Нет пересекающихся дат между контрактами.")
                rollover_dates.append(None)
                continue
            merged['volume_ratio'] = merged['volume_next'] / \
                (merged['volume_current'] + merged['volume_next'])
            crossover = merged[merged['volume_ratio'] > volume_threshold]
            if not crossover.empty:
                rollover_date = crossover.index[0]
                logging.info(
                    f"Определена дата rollover между {current_df['contract'].iloc[0]} и {next_df['contract'].iloc[0]}: {rollover_date}")
                rollover_dates.append(rollover_date)
            else:
                logging.info(
                    f"Порог объёма не достигнут между {current_df['contract'].iloc[0]} и {next_df['contract'].iloc[0]}. Используем последний общий день.")
                rollover_dates.append(merged.index[-1])
        except Exception as e:
            logging.error(
                f"Ошибка при определении rollover между контрактами: {e}")
            rollover_dates.append(None)
    return rollover_dates

# Объединение данных контрактов с учетом rollover


def combine_contracts(dfs, rollover_dates):
    combined = pd.DataFrame()
    for i, df in enumerate(dfs):
        try:
            if i == 0:
                if rollover_dates and rollover_dates[0]:
                    df_part = df[df.index <= rollover_dates[0]]
                    logging.info(
                        f"Используем данные первого контракта до {rollover_dates[0]}.")
                else:
                    df_part = df
                    logging.info(
                        "Нет rollover даты для первого контракта, используем весь контракт.")
            elif i < len(dfs)-1:
                start_date = rollover_dates[i-1]
                end_date = rollover_dates[i] if rollover_dates[i] else df.index[-1]
                df_part = df[(df.index > start_date) & (df.index <= end_date)]
                logging.info(
                    f"Используем данные контракта {df['contract'].iloc[0]} с {start_date} по {end_date}.")
            else:
                if rollover_dates and rollover_dates[i-1]:
                    start_date = rollover_dates[i-1]
                    df_part = df[df.index > start_date]
                    logging.info(
                        f"Используем данные последнего контракта {df['contract'].iloc[0]} начиная с {start_date}.")
                else:
                    df_part = df
                    logging.info(
                        "Нет rollover даты для последнего контракта, используем весь контракт.")
            combined = pd.concat([combined, df_part])
        except Exception as e:
            logging.error(
                f"Ошибка при объединении данных контракта {df['contract'].iloc[0]}: {e}")
    combined = combined.sort_index()
    logging.info(f"Объединённый DataFrame содержит {len(combined)} записей.")
    return combined

# Функция для построения графика объёмов для каждого контракта и отображения дат rollover


# Функция для построения графиков дневных объёмов по годам (для каждого года отдельно)
def plot_volume_graph_by_year(dfs, volume_threshold=0.5, figsize=(15, 5)):
    try:
        # Для каждого контракта вычисляем ежедневную агрегацию
        contract_daily = {}
        for df in dfs:
            daily = df[['volume']].resample('1D').sum()
            contract_name = df['contract'].iloc[0]
            contract_daily[contract_name] = daily

        # Собираем все года, в которых есть данные
        all_years = set()
        for daily in contract_daily.values():
            all_years.update(daily.index.year.tolist())
        all_years = sorted(all_years)

        # Вычисляем глобальные даты rollover для каждой пары подряд идущих контрактов
        global_rollover = []
        for i in range(len(dfs)-1):
            try:
                daily1 = dfs[i][['volume']].resample('1D').sum()
                daily2 = dfs[i+1][['volume']].resample('1D').sum()
                merged = daily1.merge(daily2, how='outer', left_index=True,
                                      right_index=True, suffixes=('_x', '_y')).fillna(0)
                merged['diff'] = merged['volume_y'] - merged['volume_x']
                merged['sum'] = merged['volume_y'] + merged['volume_x']
                merged['check'] = (merged['sum'] > merged['sum'].ewm(
                    10).mean()) & (merged['diff'] > 0)
                rollover_date = merged.index[merged['check'] == True][0]
                global_rollover.append(rollover_date)
            except Exception as e:
                logging.error(
                    f"Ошибка при вычислении глобального rollover для контрактов {dfs[i]['contract'].iloc[0]} и {dfs[i+1]['contract'].iloc[0]}: {e}")
                global_rollover.append(None)

        # Для каждого года строим отдельный график
        for year in all_years:
            plt.figure(figsize=figsize)
            # Отрисовываем данные для каждого контракта, если они есть в этом году
            for contract, daily in contract_daily.items():
                mask = daily.index.year == year
                daily_year = daily.loc[mask]
                if not daily_year.empty:
                    plt.plot(daily_year.index,
                             daily_year['volume'], label=contract)
            # Отрисовываем вертикальные линии rollover, если они попадают в этот год
            for r in global_rollover:
                if r is not None and r.year == year:
                    plt.axvline(r, color='red', linestyle='--',
                                label='transition date')
            plt.ylabel('Daily volume')
            plt.xlabel('Date')
            plt.title(f"График дневных объёмов для {year}")
            plt.legend()
            plt.grid(True)
            plt.show()
            logging.info(f"Отрисован график для {year}.")
    except Exception as e:
        logging.error(f"Ошибка при построении графиков по годам: {e}")


def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Склейка CSV-файлов фьючерсных контрактов в непрерывную серию.")
    parser.add_argument('--folder', type=str, default='nq',
                        help="Путь к папке с CSV-файлами.")
    parser.add_argument('--underlying', type=str, default='NQ',
                        help="Базовый актив (например, NQ).")
    parser.add_argument(
        '--output', type=str, default='continuous.csv', help="Имя итогового CSV-файла.")
    parser.add_argument('--volume_threshold', type=float,
                        default=0.5, help="Порог объёма для определения rollover.")
    args = parser.parse_args()

    folder = args.folder
    underlying = args.underlying
    output_file = args.output
    volume_threshold = args.volume_threshold

    if not os.path.exists(folder):
        logging.error(f"Папка {folder} не найдена.")
        return

    pattern = os.path.join(folder, "*.csv")
    files = glob.glob(pattern)
    if not files:
        logging.error(f"В папке {folder} не найдено CSV-файлов.")
        return

    # Извлечение информации о контрактах и фильтрация по базовому активу
    contracts_info = []
    for file in files:
        info = parse_contract_filename(file, underlying)
        if info is not None:
            contracts_info.append(info)

    if not contracts_info:
        logging.error("Нет файлов, соответствующих базовому активу.")
        return

    # Сортировка контрактов по дате истечения
    contracts_info.sort(key=lambda x: x['expiration'])
    logging.info("Контракты отсортированы по дате истечения.")

    dfs = []
    for info in contracts_info:
        df = load_contract_data(info)
        if df is not None:
            dfs.append(df)
    if not dfs:
        logging.error("Ошибка загрузки данных из файлов.")
        return

    rollover_dates = determine_rollover_dates(
        dfs, volume_threshold=volume_threshold)
    if rollover_dates:
        logging.info("Даты rollover определены.")
    else:
        logging.info(
            "Даты rollover не определены, будет использована полная история каждого контракта.")

    continuous_df = combine_contracts(dfs, rollover_dates)
    # Приведение столбца 'contract': удаляем расширение ".csv"
    continuous_df['contract'] = continuous_df['contract'].str.replace(
        '.csv', '', regex=False)

    # Удаляем столбец 'expiration'
    if 'expiration' in continuous_df.columns:
        continuous_df.drop(columns=['expiration'], inplace=True)

    try:
        continuous_df.to_csv(output_file)
        logging.info(f"Объединённый файл сохранён как {output_file}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении файла {output_file}: {e}")

    # Построение графиков дневных объёмов для каждого года
    # plot_volume_graph_by_year(dfs, volume_threshold=volume_threshold, figsize=(15, 5))


# %%
main()
