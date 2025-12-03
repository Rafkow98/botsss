import os
import sys

from discord import LoginFailure
from pymysql import OperationalError

from bot import run_bot

from dotenv import load_dotenv

load_dotenv()
DC_TOKEN = os.getenv('DC_TOKEN')
SQL_LOGIN = os.getenv('SQL_LOGIN')
SQL_PASS = os.getenv('SQL_PASS')
HOST = os.getenv('HOST')
DB_NAME = os.getenv('DB_NAME')
GARAGE_DB_NAME = os.getenv('GARAGE_DB_NAME')


def main():
    try:
        bot_connection_string = f'mysql+pymysql://{SQL_LOGIN}:{SQL_PASS}@{HOST}:3306/{DB_NAME}?charset=utf8mb4'
        garage_connection_string = f'mysql+pymysql://{SQL_LOGIN}:{SQL_PASS}@{HOST}:3306/{GARAGE_DB_NAME}?charset=utf8mb4'
        run_bot(bot_connection_string, garage_connection_string, DC_TOKEN)
    except IndexError:
        print('Nieprawidłowa liczba argumentów')
        sys.exit()
    except OperationalError:
        print('Nieprawidłowe dane logowania')
        sys.exit()
    except LoginFailure:
        print('Nieprawidłowy token bota')
        sys.exit()


if __name__ == '__main__':
    main()
