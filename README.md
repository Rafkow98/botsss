# SimSprintSeries Discord Bot

This bot is created for [SimSprintSeries server](https://www.simss.pl/discord).

## Programs needed to run the bot

- Python 3.10
- MariaDB 11.8

## Starting

Run those commands in your shell, in the bot's directory. The ```pip install``` command might be run only once, there is no need to run it every time

```bash
pip install -r requirements.txt
python main.py [MARIADB_USER] [MARIADB_PASSWORD] [HOST] [DATABASE_NAME] [BOT_TOKEN]
```

## Functionalities

The bot tracks every message and every reaction added by users and logs edited or deleted messages. It also can send user's avatar or count percentage of toxic messages sent by specific user

The bot can be customized by changing user and channel ID's, as well as gifs and links from the bot.py file.