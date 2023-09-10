import pandas as pd
from loguru import logger
import config as cfg
from sqlalchemy import create_engine


def set_player_starts_train(db_connection):
    logger.warning("Перенос player_starts_train.parquet стартовал успешно! Пожалуйста подождите...")

    df = pd.read_parquet('YourDataForConvert/player_starts_train.parquet', engine='fastparquet')
    df.to_sql("player_starts_train", db_connection, if_exists='replace')

    logger.warning("Перенос player_starts_train.parquet успешно завершен!")


def set_small_player_starts_train(db_connection):
    logger.warning("Перенос small_player_starts_train.csv стартовал успешно! Пожалуйста подождите...")

    df = pd.read_csv('YourDataForConvert/small_player_starts_train.csv')
    df.to_sql("player_starts_train", db_connection, if_exists='replace')

    logger.warning("Перенос small_player_starts_train.csv успешно завершен!")


def set_emotions(db_connection):
    logger.warning("Перенос emotions.csv стартовал успешно! Пожалуйста подождите...")

    df = pd.read_csv('YourDataForConvert/emotions.csv')
    df.to_sql("emotions", db_connection, if_exists='replace')

    logger.warning("Перенос emotions.csv успешно завершен!")


def set_videos(db_connection):
    logger.warning("Перенос videos.parquet стартовал успешно! Пожалуйста подождите...")

    df = pd.read_parquet('YourDataForConvert/videos.parquet')
    df.to_sql("videos", db_connection, if_exists='replace')

    logger.warning("Перенос videos.parquet успешно завершен!")


if __name__ == "__main__":

    while True:
        try:
            answer = input('Выберите вариант загрузки данных:'
                           '\n1 - Перенести все данные в БД\n2 - Только player_starts_train.parquete\n'
                           '3 - Только videos.parquet\n4 - только emotions.csv\nВведите порядковый номер подходящего варианта: ')

            db = create_engine(
                f'postgresql+psycopg2://{cfg.DB_USERNAME}:{cfg.DB_PASSWORD}@{cfg.DB_HOST}:{cfg.DB_PORT}/{cfg.DB_NAME}'
            )

            if answer == '1':
                set_emotions(db)
                set_videos(db)
                set_player_starts_train(db)

            elif answer == '2':
                set_player_starts_train(db)

            elif answer == '3':
                set_videos(db)

            elif answer == '4':
                set_emotions(db)

            else:
                print("Неверный выбор. Попробуйте еще раз!")
        except (KeyboardInterrupt, SystemExit):
            break