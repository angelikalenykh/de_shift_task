import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
import json
from datetime import datetime
import logging
import time
from datetime import datetime
from datetime import timedelta


def main():
    try:
        # Загрузка параметров
        logger.info("Загрузка параметров из parametrs.json")
        with open('parametrs.json') as f:
            params = json.load(f)
        
        # Получение данных
        logger.info("Запрашиваю данные с API...")
        weather_data = fetch_weather_data(params)
        
        # Трансформация данных
        logger.info("Обрабатываю данные...")
        df = transform_data(weather_data)
        logger.info("Данные успешно преобразованы")

        # Запрос способа сохранения
        print("\nВыберите способ сохранения данных:")
        print("1 - Сохранить в CSV файл")
        print("2 - Загрузить в базу данных")
        print("3 - Оба варианта")
        print("0 - Не сохранять")
        
        while True:
            choice = input("Ваш выбор (0-3): ")
            
            if choice == '1':
                # Сохранение в CSV
                try:
                    filename = input("Введите имя файла (по умолчанию weather.csv): ") or "weather.csv"
                    df.to_csv(filename, index=False)
                    logger.info(f"Данные сохранены в {filename}")
                except Exception as e:
                    logger.error(f"Ошибка сохранения в CSV: {e}")
                break
                
            elif choice == '2':
                # Загрузка в БД
                try:
                    logger.info("Загружаю данные в PostgreSQL...")
                    load_data_to_db(df, DB_CONFIG)
                except Exception as e:
                    logger.error(f"Ошибка загрузки в БД: {e}")
                break
                
            elif choice == '3':
                # Оба варианта
                try:
                    filename = input("Введите имя файла (по умолчанию weather.csv): ") or "weather.csv"
                    df.to_csv(filename, index=False)
                    logger.info(f"Данные сохранены в {filename}")
                    
                    logger.info("Загружаю данные в PostgreSQL...")
                    load_data_to_db(df, DB_CONFIG)
                except Exception as e:
                    logger.error(f"Ошибка при сохранении: {e}")
                break
                
            elif choice == '0':
                logger.info("Сохранение отменено")
                break
                
            else:
                print("Неверный ввод! Пожалуйста, выберите 0-3")

        logger.info("Готово!")
        
    except Exception as e:
        logger.error(f"Критическая ошибка в main: {e}")

"""

Получает данные погоды через API по введенным пользователем данным


"""

def input_date(prompt):
    """Функция для ввода даты с проверкой формата"""
    while True:
        date_str = input(prompt + " (в формате ГГГГ-ММ-ДД): ")
        try:
            # Пробуем преобразовать введенную строку в дату
            datetime.strptime(date_str, "%Y-%m-%d")
            return date_str
        except ValueError:
            print("Ошибка! Неверный формат даты. Попробуйте снова.")

# Получаем даты от пользователя с проверкой
GLOBAL_START_DATE = input_date("Укажите дату начала периода")
GLOBAL_END_DATE = input_date("Укажите дату окончания периода")

# Проверяем, что конечная дата не раньше начальной
while GLOBAL_END_DATE < GLOBAL_START_DATE:
    print("Ошибка! Дата окончания не может быть раньше даты начала.")
    GLOBAL_END_DATE = input_date("Укажите корректную дату окончания периода")

print(f"\nУстановлен период: с {GLOBAL_START_DATE} по {GLOBAL_END_DATE}")

def fetch_weather_data(params):
    try:
        # Преобразуем массивы в строки с запятыми
        daily_params = ",".join(params["daily"]) if isinstance(params["daily"], list) else params["daily"]
        hourly_params = ",".join(params["hourly"]) if isinstance(params["hourly"], list) else params["hourly"]

        load_start_date = (pd.to_datetime(GLOBAL_START_DATE) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        load_end_date = (pd.to_datetime(GLOBAL_END_DATE) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": params["latitude"],
                "longitude": params["longitude"],
                "daily": daily_params,
                "hourly": hourly_params,
                "timezone": params["timezone"],
                # Берем даты из глобальных переменных
                "start_date": load_start_date,
                "end_date": load_end_date,
                "temperature_unit": params["temperature_unit"],
                "wind_speed_unit": params["wind_speed_unit"],
                "precipitation_unit": params["precipitation_unit"],
                "timeformat": params.get("timeformat", "unixtime")
            }
        )
        response.raise_for_status()
        logger.info("Данные успешно получены с API")
        return response.json()
    except Exception as e:
        logger.error(f"Ошибка при запросе к API: {e}")
        raise


""" 

Функции для конвертации единиц измерения
Преобразует сырые данные из API в формат для таблицы weather
Возвращает pandas DataFrame с нужной структурой

"""
# Функции для конвертации единиц измерения
def fahrenheit_to_celsius(f):
    return (f - 32) * 5/9 if f is not None else None

def knots_to_mps(knots):
    return knots * 0.514444 if knots is not None else None

def inches_to_mm(inches):
    return inches * 25.4 if inches is not None else None

def feet_to_meters(feet):
    return feet * 0.3048 if feet is not None else None

def transform_data(json_data):
    """
    Преобразует сырые данные из API в формат для таблицы weather
    Возвращает pandas DataFrame с нужной структурой
    """
    try:
        # Создаем DataFrame из полученных данных
        hourly_data = pd.DataFrame(json_data['hourly'])
        daily_data = pd.DataFrame(json_data['daily'])
        
        # Конвертация времени
        hourly_data['time'] = pd.to_datetime(hourly_data['time'], unit='s')
        hourly_data['date'] = hourly_data['time'].dt.date
        
        daily_data['time'] = pd.to_datetime(daily_data['time'], unit='s')
        daily_data['sunrise'] = pd.to_datetime(daily_data['sunrise'], unit='s')
        daily_data['sunset'] = pd.to_datetime(daily_data['sunset'], unit='s')
        daily_data['date'] = daily_data['time'].dt.date
        
        # Объединение данных
        merged_data = pd.merge(hourly_data, daily_data, on='date', how='left')
        
        # Преобразование единиц измерения
        merged_data['temperature_2m_celsius'] = merged_data['temperature_2m'].apply(fahrenheit_to_celsius)
        merged_data['dew_point_2m'] = merged_data['dew_point_2m'].apply(fahrenheit_to_celsius)
        merged_data['apparent_temperature_celsius'] = merged_data['apparent_temperature'].apply(fahrenheit_to_celsius)
        merged_data['temperature_80m_celsius'] = merged_data['temperature_80m'].apply(fahrenheit_to_celsius)
        merged_data['temperature_120m_celsius'] = merged_data['temperature_120m'].apply(fahrenheit_to_celsius)
        merged_data['soil_temperature_0cm_celsius'] = merged_data['soil_temperature_0cm'].apply(fahrenheit_to_celsius)
        merged_data['soil_temperature_6cm_celsius'] = merged_data['soil_temperature_6cm'].apply(fahrenheit_to_celsius)
        
        merged_data['wind_speed_10m_m_per_s'] = merged_data['wind_speed_10m'].apply(knots_to_mps)
        merged_data['wind_speed_80m_m_per_s'] = merged_data['wind_speed_80m'].apply(knots_to_mps)
        
        merged_data['visibility'] = merged_data['visibility'].apply(feet_to_meters)
        
        merged_data['rain_mm'] = merged_data['rain'].apply(inches_to_mm)
        merged_data['showers_mm'] = merged_data['showers'].apply(inches_to_mm)
        merged_data['snowfall_mm'] = merged_data['snowfall'].apply(inches_to_mm)
        
        # Определение дневного времени
        merged_data['is_daylight'] = (merged_data['time_x'] >= merged_data['sunrise']) & (merged_data['time_x'] <= merged_data['sunset'])
        
        # Расчет daylight_hours в часах
        daily_data['daylight_hours'] = daily_data['daylight_duration'] / 3600
        
        # Агрегация данных по дням
        daily_stats = merged_data.groupby('date').agg({
            'temperature_2m_celsius': 'mean',
            'relative_humidity_2m': 'mean',
            'dew_point_2m': 'mean',
            'apparent_temperature_celsius': 'mean',
            'temperature_80m_celsius': 'mean',
            'temperature_120m_celsius': 'mean',
            'wind_speed_10m_m_per_s': 'mean',
            'wind_speed_80m_m_per_s': 'mean',
            'visibility': 'mean',
            'rain_mm': 'sum',
            'showers_mm': 'sum',
            'snowfall_mm': 'sum'
        }).reset_index()
        
        daily_stats.columns = [
            'date', 'avg_temperature_2m_24h', 'avg_relative_humidity_2m_24h',
            'avg_dew_point_2m_24h', 'avg_apparent_temperature_24h',
            'avg_temperature_80m_24h', 'avg_temperature_120m_24h',
            'avg_wind_speed_10m_24h', 'avg_wind_speed_80m_24h',
            'avg_visibility_24h', 'total_rain_24h', 'total_showers_24h',
            'total_snowfall_24h'
        ]
        
        # Агрегация для дневного времени
        daylight_stats = merged_data[merged_data['is_daylight']].groupby('date').agg({
            'temperature_2m_celsius': 'mean',
            'relative_humidity_2m': 'mean',
            'dew_point_2m': 'mean',
            'apparent_temperature_celsius': 'mean',
            'temperature_80m_celsius': 'mean',
            'temperature_120m_celsius': 'mean',
            'wind_speed_10m_m_per_s': 'mean',
            'wind_speed_80m_m_per_s': 'mean',
            'visibility': 'mean',
            'rain_mm': 'sum',
            'showers_mm': 'sum',
            'snowfall_mm': 'sum'
        }).reset_index()
        
        daylight_stats.columns = [
            'date', 'avg_temperature_2m_daylight', 'avg_relative_humidity_2m_daylight',
            'avg_dew_point_2m_daylight', 'avg_apparent_temperature_daylight',
            'avg_temperature_80m_daylight', 'avg_temperature_120m_daylight',
            'avg_wind_speed_10m_daylight', 'avg_wind_speed_80m_daylight',
            'avg_visibility_daylight', 'total_rain_daylight',
            'total_showers_daylight', 'total_snowfall_daylight'
        ]
        
        # Объединение всех данных
        final_data = pd.merge(daily_stats, daylight_stats, on='date', how='left')
        final_data = pd.merge(final_data, daily_data[['date', 'daylight_hours', 'sunrise', 'sunset']], on='date')
        
        # Добавление последних измерений за день
        last_measurements = merged_data.sort_values('time_x').groupby('date').last().reset_index()
        final_data = pd.merge(final_data, last_measurements[[
            'date', 'temperature_2m_celsius', 'apparent_temperature_celsius',
            'temperature_80m_celsius', 'temperature_120m_celsius',
            'soil_temperature_0cm_celsius', 'soil_temperature_6cm_celsius',
            'rain_mm', 'showers_mm', 'snowfall_mm',
            'wind_speed_10m_m_per_s', 'wind_speed_80m_m_per_s'
        ]], on='date', how='left')

        
        final_data = final_data[
            (final_data['date'] >= pd.to_datetime(GLOBAL_START_DATE).date()) &
            (final_data['date'] <= pd.to_datetime(GLOBAL_END_DATE).date())
            ]
        
        # Форматирование времени
        final_data['sunrise_iso'] = final_data['sunrise'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        final_data['sunset_iso'] = final_data['sunset'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Упорядочивание столбцов
        columns_order = [
            'date', 'avg_temperature_2m_24h', 'avg_relative_humidity_2m_24h',
            'avg_dew_point_2m_24h', 'avg_apparent_temperature_24h',
            'avg_temperature_80m_24h', 'avg_temperature_120m_24h',
            'avg_wind_speed_10m_24h', 'avg_wind_speed_80m_24h',
            'avg_visibility_24h', 'total_rain_24h', 'total_showers_24h',
            'total_snowfall_24h', 'avg_temperature_2m_daylight',
            'avg_relative_humidity_2m_daylight', 'avg_dew_point_2m_daylight',
            'avg_apparent_temperature_daylight', 'avg_temperature_80m_daylight',
            'avg_temperature_120m_daylight', 'avg_wind_speed_10m_daylight',
            'avg_wind_speed_80m_daylight', 'avg_visibility_daylight',
            'total_rain_daylight', 'total_showers_daylight',
            'total_snowfall_daylight', 'temperature_2m_celsius',
            'apparent_temperature_celsius', 'temperature_80m_celsius',
            'temperature_120m_celsius', 'soil_temperature_0cm_celsius',
            'soil_temperature_6cm_celsius', 'rain_mm', 'showers_mm',
            'snowfall_mm', 'wind_speed_10m_m_per_s', 'wind_speed_80m_m_per_s',
            'daylight_hours', 'sunrise_iso', 'sunset_iso'
        ]
        
        # Заполнение NULL значений
        final_data = final_data[columns_order].fillna(pd.NA)
        
        return final_data
    
    except Exception as e:
        logger.error(f"Ошибка при трансформации данных: {e}")
        raise

"""

Загружает данные в таблицу weather в PostgreSQL
Обрабатывает дубликаты через ON CONFLICT UPDATE

"""

def load_data_to_db(df, db_config):
    conn = None
    try:
        # Подключение к БД
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Подготовка данных
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        data_tuples = []
        
        # Преобразование NA в None для PostgreSQL
        for row in df.itertuples(index=False):
            data_tuples.append(tuple(None if pd.isna(x) else x for x in row))
        
        # SQL-запрос с правильным форматированием
        columns = [
            'date', 'avg_temperature_2m_24h', 'avg_relative_humidity_2m_24h',
            'avg_dew_point_2m_24h', 'avg_apparent_temperature_24h',
            'avg_temperature_80m_24h', 'avg_temperature_120m_24h',
            'avg_wind_speed_10m_24h', 'avg_wind_speed_80m_24h',
            'avg_visibility_24h', 'total_rain_24h', 'total_showers_24h',
            'total_snowfall_24h', 'avg_temperature_2m_daylight',
            'avg_relative_humidity_2m_daylight', 'avg_dew_point_2m_daylight',
            'avg_apparent_temperature_daylight', 'avg_temperature_80m_daylight',
            'avg_temperature_120m_daylight', 'avg_wind_speed_10m_daylight',
            'avg_wind_speed_80m_daylight', 'avg_visibility_daylight',
            'total_rain_daylight', 'total_showers_daylight',
            'total_snowfall_daylight', 'temperature_2m_celsius',
            'apparent_temperature_celsius', 'temperature_80m_celsius',
            'temperature_120m_celsius', 'soil_temperature_0cm_celsius',
            'soil_temperature_6cm_celsius', 'rain_mm', 'showers_mm',
            'snowfall_mm', 'wind_speed_10m_m_per_s', 'wind_speed_80m_m_per_s',
            'daylight_hours', 'sunrise_iso', 'sunset_iso'
        ]
        
        insert_query = f"""
            INSERT INTO weather ({','.join(columns)})
            VALUES %s
            ON CONFLICT (date) DO UPDATE SET
                {','.join([f"{col}=EXCLUDED.{col}" for col in columns if col != 'date'])}
        """
        
        # Используем execute_values для пакетной вставки
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()
        logger.info(f"Успешно загружено {len(data_tuples)} записей в БД")
        
    except (Exception, psycopg2.DatabaseError) as e:
        logger.error(f"Ошибка при загрузке в БД: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


"""

Настройка логирования

"""
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('weather_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Константы для подключения к БД
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'weather_db',
    'user': 'weather_user',
    'password': 'weather_pass'
}

"""

Ожидает готовности базы данных

"""

def wait_for_db(max_retries=5, delay=5):
    
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            logger.info("База данных готова")
            return True
        except psycopg2.OperationalError as e:
            retries += 1
            logger.warning(f"Попытка {retries}/{max_retries} - база данных не готова: {e}")
            time.sleep(delay)
    logger.error("Не удалось подключиться к базе данных")
    return False


if __name__ == "__main__":
    if wait_for_db():
        main()
    else:
        logger.error("Не удалось подключиться к базе данных. Завершение работы.")