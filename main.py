import sys

from discord import LoginFailure
from pymysql import OperationalError

from bot import run_bot


def main():
    args = sys.argv[1:]
    try:
        connection_string = f'mariadb+pymysql://{args[0]}:{args[1]}@{args[2]}:3306/{args[3]}?charset=utf8mb4'
        run_bot(connection_string, args[4])
    except IndexError:
        print('Nieprawidłowa liczba argumentów - prawidłowa kolejność: "użytkownik hasło host baza token"')
        sys.exit()
    except OperationalError:
        print('Nieprawidłowe dane logowania - prawidłowa kolejność: "użytkownik hasło host baza token"')
        sys.exit()
    except LoginFailure:
        print('Nieprawidłowy token bota - prawidłowa kolejność: "użytkownik hasło host baza token"')
        sys.exit()


if __name__ == '__main__':
    main()
