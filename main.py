import logging
import os
import sys

from discord import LoginFailure
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import URL

from bot import run_bot

from dotenv import load_dotenv

load_dotenv()
DC_TOKEN = os.getenv('DC_TOKEN')
SQL_LOGIN = os.getenv('SQL_LOGIN')
SQL_PASS = os.getenv('SQL_PASS')
HOST = os.getenv('HOST')
PORT = int(os.getenv('DB_PORT') or 3306)
DB_NAME = os.getenv('DB_NAME')
GARAGE_DB_NAME = os.getenv('GARAGE_DB_NAME')
GARAGE_SQL_LOGIN = os.getenv('GARAGE_SQL_LOGIN') or SQL_LOGIN
GARAGE_SQL_PASS = os.getenv('GARAGE_SQL_PASS') or SQL_PASS
DB_SSL_CA = os.getenv('DB_SSL_CA')


def build_url(user, password, database):
    return URL.create(
        drivername='mysql+pymysql',
        username=user,
        password=password,
        host=HOST,
        port=PORT,
        database=database,
        query={'charset': 'utf8mb4'},
    )

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    connect_args = {'ssl': {'ca': DB_SSL_CA}} if DB_SSL_CA else {}
    try:
        run_bot(build_url(SQL_LOGIN, SQL_PASS, DB_NAME),
                build_url(GARAGE_SQL_LOGIN, GARAGE_SQL_PASS, GARAGE_DB_NAME),
                DC_TOKEN,
                connect_args=connect_args)

    except OperationalError:
        print('Nie można połączyć się z bazą danych (dane logowania / host / TLS)')
        sys.exit(1)
    except LoginFailure:
        print('Nieprawidłowy token bota')
        sys.exit(1)


if __name__ == '__main__':
    main()
