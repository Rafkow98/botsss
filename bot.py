import asyncio
import logging
import os
import random
import re
from datetime import timedelta, datetime, timezone

import pandas as pd

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, bindparam
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv()

log = logging.getLogger('simss-bot')

SAFE_MENTIONS = discord.AllowedMentions(everyone=False, users=False, roles=False, replied_user=True)
NO_MENTIONS = discord.AllowedMentions.none()
DISCORD_MSG_LIMIT = 2000
EMBED_FIELD_LIMIT = 1024

def run_bot(bot_connection_string, garage_connection_string, token, connect_args = None):
    intents = discord.Intents.all()
    intents.members = True

    bot = commands.Bot(command_prefix="!", intents=intents, allowed_mentions=SAFE_MENTIONS)
    bot.remove_command("help")

    scheduler = AsyncIOScheduler()

    connect_args = connect_args or {}

    bot_engine = create_engine(bot_connection_string, pool_pre_ping=True)
    garage_engine = create_engine(garage_connection_string, pool_pre_ping=True)

    for engine in (bot_engine, garage_engine):
        with engine.connect():
            pass

    bots = [968851237405597717, 475744554910351370, 235148962103951360, 762217899355013120, 159985870458322944,
            1311665961027244084, 734535151899639910, 1311665961027244084, 1211781489931452447]

    gifs = [
        'https://media.discordapp.net/attachments/902300785466040370/980959555666182194/kowalski.gif?ex=6779b4c7&is=67786347&hm=bbc24b6f3bf4f57baff91e56d2fb47f1d40c2537a9af1ff610fd3b0de3350e9f&',
        'https://cdn.discordapp.com/attachments/815522218213244958/1327760435856015511/ezgif-5-3b1bf3bb1d.gif?ex=67843cc4&is=6782eb44&hm=48e92cc855316c44ebfd867fcf28bb66f85b778f0560d131f50321e1722dccf9&',
        'https://media.discordapp.net/attachments/834503336979857471/1231702797871742976/ezgif.com-animated-gif-maker.gif?ex=677ce55e&is=677b93de&hm=2b95d9277e621cd4ed0340818a887faf69fe6c0f6a5ac97d2aed8c48b2f34566&=',
        'https://media.discordapp.net/attachments/1159912825473466531/1186340488148688916/attachment.gif?ex=67ad716d&is=67ac1fed&hm=a75c50d1d00577f3771a3a5ae16d17e8baf13c4cecb52094d0c44cbd296059a6&',
        'https://cdn.discordapp.com/attachments/738723839759876149/1296002243165814784/reeporage.gif?ex=67ad9625&is=67ac44a5&hm=825b20c5bccdae02a8fab5c152e7918f9dcf0f144f4179a31ce8f11171f99123&',
        'https://media.discordapp.net/attachments/733758693971066963/1353073468991868999/2025-03-2219-22-31-ezgif.com-video-to-gif-converter.gif?ex=67e24d9d&is=67e0fc1d&hm=9939698539cbbb09f3892601cd3b91f0882e567ea2f4615c0b5c092ad99235a1&=',
        'https://media.discordapp.net/attachments/893248676082900994/903014221590175744/ffd7b3f9-dc66-43c2-b11d-a07d120ec320.gif?ex=67bb8207&is=67ba3087&hm=7fe381f03e34186ccc0f4030e4d3c68a9e8f4d5a697bb24b0cf2cde46a16958b&=']

    czysto = [
        'https://media.discordapp.net/attachments/995375885781831730/1108518508733673473/ezgif.com-video-to-gif_1.gif?ex=677bad8f&is=677a5c0f&hm=f460b598f6a0a501657f6ddfebc21fd9929ca3923342a4bffa199bb878289055&=',
        'https://media.discordapp.net/attachments/1022188669710373017/1159594752795226273/ezgif.com-video-to-gif.gif?ex=677b9a85&is=677a4905&hm=e81b495e3763ef963b1728fda783ba1bed5681fd7913341f737b406a48a27242&=',
        'https://media.discordapp.net/attachments/1022188669710373017/1174820045495468153/ezgif.com-video-to-gif.gif?ex=677b9f2d&is=677a4dad&hm=bb06dadfe9da96f1ff06f6f2be2d5fa3b965bb34a64b642099078b416d4f0632&=',
        'https://media.discordapp.net/attachments/1022188669710373017/1111241957159731310/ezgif.com-video-to-gif.gif?ex=677bb2b8&is=677a6138&hm=59bd101cb84c9f4efb7f44b9f8ab01ffba2fd1bc8092c1d3486b674c4767969f&=',
        'https://media.discordapp.net/attachments/995375885781831730/1179185410820280430/ezgif.com-video-to-gif.gif?ex=677baebe&is=677a5d3e&hm=891abc06128ad0dcc20a89a65fa120473bf65db2d88ecf81120e62ceff4a7db2&=',
        'https://media.discordapp.net/attachments/733940138283106396/1170773295738654780/ezgif.com-video-to-gif.gif?ex=677b66d9&is=677a1559&hm=31a2b6ab23327cec2ce52562216240879bb71795ac7b48c0b7f6aa2934d8b88e&=',
        'https://media.discordapp.net/attachments/995375885781831730/1316888151037444116/ezgif-1-0f282c535f.gif?ex=677baa69&is=677a58e9&hm=97d3cf2c73593bbc7f71fad28b40bd8a8cbbc5a715d54e738b9999bb517572be&=&width=400&height=225']

    toxic_words = ['huj', 'cip', 'pierda', 'pierdo', 'dup', 'kurew', 'kurw', 'kutas', 'pizd', 'jeb', 'rucha', 'sra']
    racist_words = ['niger', 'nigger', 'niga', 'nyga', 'nigga', 'czarnuch', 'murzyn', 'rasis', 'rasiz']

    invite_pattern = re.compile(r"(?:discord(?:app)?\s*\.\s*(?:gg | com\s * /\s * invite | io | me | li) | dsc\s *\.\s * gg "
                                r"| invite\s *\.\s * gg)\s * /\s * [A - Zaz0 - 9 -] + ", re.IGNORECASE,)

    admins = [523929325171638280, 146344154887094273, 917064080366391386, 686636820196491305, 1376320417890963546, 308273688208211968, 1288993340532064338, 573780343488905221]

    trapped_users = set()

    def is_privileged(member):
        if member.id in admins:
            return True
        return isinstance(member, discord.Member) and member.guild_permissions.manage_messages

    def classify(content):
        content = (content or '').lower()
        return (any(w in content for w in toxic_words),
                any(w in content for w in racist_words))

    def update_message_counts(author_id, content, delta):
        is_toxic, is_racist = classify(content)
        sets = ["`all` = `all` + :d", "all_24 = all_24 + :d"]
        if is_toxic:
            sets += ["toxic = toxic + :d", "toxic_24 = toxic_24 + :d"]
        if is_racist:
            sets += ["racism = racism + :d", "racism_24 = racism_24 + :d"]
        with bot_engine.begin() as cnx:
            cnx.execute(text(f"UPDATE messages_count SET {', '.join(sets)} WHERE author_id =:id"),
            {'d': delta, 'id': author_id})

    async def run_db(func, *args):
        return await asyncio.to_thread(func, *args)

    async def send_log(content):
        channel = bot.get_channel(int(os.getenv('BIN_CHANNEL_ID')))
        if channel is None:
            log.warning("Nie znaleziono kanału BIN_CHANNEL_ID")
            return
        chunk = DISCORD_MSG_LIMIT - 50
        for i in range(0, len(content), chunk):
            try:
                await channel.send(content[i:i + chunk], allowed_mentions=NO_MENTIONS)
            except discord.HTTPException as e:
                log.warning("Nie udało się wysłać logu: %s", e)
            return

    async def check_invite(message):
        if not invite_pattern.search(message.content or ''):
            return False
        if is_privileged(message.author):
            return False
        try:
            await message.delete()
        except discord.NotFound:
            pass
        except discord.Forbidden:
            log.warning("Brak uprawnień do usunięcia zaproszenia w kanale %s", message.channel.id)
            return False
        await message.channel.send(f'{message.author.mention}, wysyłanie zaproszeń jest zabronione na tym serwerze.',
            allowed_mentions = discord.AllowedMentions(users=[message.author]))
        return True

    def name_or_mention(guild, user_id):
        member = guild.get_member(int(user_id)) if guild else None
        return member.display_name if member else f"<@{user_id}>"

    def field_value(names):
        value = "\n".join(names) or "—"
        if len(value) > EMBED_FIELD_LIMIT:
            value = value[:EMBED_FIELD_LIMIT - 1] + "…"
        return value

    @bot.event
    async def on_ready():
        if not event_checker.is_running:
            event_checker.start()

        channels_list = []
        members_list = []
        for guild in bot.guilds:
            if guild.id == int(os.getenv('SERVER_ID')):
                for channel in guild.text_channels:
                    channels_list.append({'id': channel.id, 'name': channel.name})
                for member in guild.members:
                    members_list.append({'id': member.id, 'name': member.name, 'avatar': member.avatar})

        channels = pd.DataFrame(channels_list)
        channels.to_sql(name='channels_temp', con=bot_engine, if_exists='replace', index=False)
        with bot_engine.begin() as cnx:
            cnx.execute(text("UPDATE channels SET is_active = 0"))
            cnx.execute(text("UPDATE channels SET is_active = 1 "
                             "WHERE id IN (SELECT id FROM channels_temp)"))
            cnx.execute(text("DROP TABLE channels_temp"))

        members = pd.DataFrame(members_list)
        members.to_sql(name='members_temp', con=bot_engine, if_exists='replace', index=False)
        with bot_engine.begin() as cnx:
            cnx.execute(text("UPDATE members SET is_on_server = 0"))
            cnx.execute(text("UPDATE members SET is_on_server = 1 "
                             "WHERE id IN (SELECT id FROM members_temp)"))
            cnx.execute(text("DROP TABLE members_temp"))

        asyncio.create_task(schedule_reminders())
        if not scheduler.running:
            scheduler.start()
        scheduler.add_job(schedule_reminders, "interval", minutes=30, id="refresh_reminders", replace_existing=True)

    @bot.event
    async def on_member_join(member):
        with bot_engine.begin() as cnx:
            cnx.execute(text("INSERT IGNORE INTO members(id) VALUES(:id)"), {'id': member.id})
            cnx.execute(text("UPDATE members SET name = :name, avatar = :avatar, is_on_server = 1 WHERE id = :id"),
                        {'id': member.id, 'name': member.name, 'avatar': member.avatar})

    @bot.event
    async def on_member_remove(member):
        with bot_engine.begin() as cnx:
            cnx.execute(text("UPDATE members SET is_on_server=0 WHERE id = :id"), {'id': member.id})

    @bot.event
    async def on_message(message):
        if message.author.bot:
            return
        if message.guild is None:
            await bot.process_commands(message)
            return
        if message.channel.id == int(os.getenv('QUOTES_CHANNEL_ID')) and len(message.attachments) == 0:
            try:
                await message.delete()
            except discord.NotFound:
                pass
        if message.channel.id == int(os.getenv('TRAP_CHANNEL_ID')):
            await handle_trap(message)

        params = {'id': message.id, 'type': message.type, 'created': message.created_at, 'edited': message.edited_at,
                  'pinned': message.pinned, 'content': message.content, 'author': message.author.id,
                  'channel': message.channel.id, 'attachments': bool(message.attachments), 'embeds': bool(message.embeds),
                  'stickers': bool(message.stickers), 'mentions': bool(message.mentions)}

        await run_db(store_message, params, message.content)
        await check_invite(message)
        await bot.process_commands(message)

    def store_message(params, content):
        with bot_engine.begin() as cnx:
            cnx.execute(text("INSERT IGNORE INTO messages(message_id, type, timestamp, timestampEdited, isPinned, "
                             "content, author_id, channel_id, attachments, embeds, stickers, mentions) "
                             "VALUES(:id, :type, :created, :edited, :pinned, :content, :author, :channel, "
                             ":attachments, :embeds, :stickers,:mentions)"), params)

        update_message_counts(params['author'], content, 1)

    @bot.event
    async def on_message_edit(before, after):
        if after.author.bot or after.guild is None or before.content == after.content:
            return

        await send_log(f"*Edytowana wiadomość użytkownika **{discord.utils.escape_markdown(before.author.name)}** *\n"
            f"Stara wersja: {before.content}\n"
            f"Nowa wersja: {after.content}\n"
            f"Wiadomość: {after.jump_url}")

        await check_invite(after)

        try:
            await run_db(update_edited_message, after.id, after.content, bool(after.attachments), bool(after.embeds),
                         bool(after.stickers), bool(after.mentions))
        except Exception:
            log.exception("Nie udało się zaktualizować edytowanej wiadomości %s", after.id)

    def update_edited_message(message_id, content, attachments, embeds,
                              stickers, mentions):
        with bot_engine.begin() as cnx:
            cnx.execute(text("UPDATE messages SET content = :new, attachments =:attachments, embeds =:embeds, "
                             "stickers = :stickers, mentions = :mentions WHERE message_id =:id"),
                {'new': content, 'attachments': attachments, 'embeds': embeds, 'stickers': stickers,
                 'mentions': mentions, 'id': message_id})

    @bot.event
    async def on_message_delete(message):
        if message.author.bot or message.guild is None:
            return
        await send_log(f"*Usunięta wiadomość użytkownika **{discord.utils.escape_markdown(message.author.name)} ** *\n"
            f"{message.content}")
        try:
            await run_db(delete_stored_message, message.id, message.author.id, message.content)
        except Exception:
            log.exception("Nie udało się usunąć wiadomości %s z bazy", message.id)

    def delete_stored_message(message_id, author_id, content):
        with bot_engine.begin() as cnx:
            cnx.execute(text("DELETE FROM messages WHERE message_id = :id"),
                        {'id': message_id})
        update_message_counts(author_id, content, -1)

    def get_stored_author(message_id):
        with bot_engine.connect() as cnx:
            row = cnx.execute(text("SELECT author_id FROM messages WHERE message_id =:id"), {'id': message_id}).fetchone()
        return row[0] if row else None

    async def resolve_message_author(payload):
        author_id = getattr(payload, 'message_author_id', None)
        if author_id:
            return author_id
        author_id = await run_db(get_stored_author, payload.message_id)
        if author_id:
            return author_id
        channel = bot.get_channel(payload.channel_id)
        if channel is None:
            return None
        try:
            message = await channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.Forbidden):
            return None
        return message.author.id

    def store_reaction(message_id, reaction_id, user_id, author_id, delta):
        with bot_engine.begin() as cnx:
            if delta > 0:
                cnx.execute(text("INSERT IGNORE INTO reactions(message_id, reaction_id, reacting_user_id, author_id) "
                                 "VALUES(:message_id, :reaction_id, :r_user_id, :author_id)"),
                            {'message_id': message_id, 'reaction_id': reaction_id, 'r_user_id': user_id, 'author_id': author_id})
            else:
                cnx.execute(text("DELETE FROM reactions WHERE message_id = :message_id AND reaction_id =:reaction_id "
                                 "AND reacting_user_id = :r_user_id AND author_id =:author_id"),
                            {'message_id': message_id, 'reaction_id': reaction_id, 'r_user_id': user_id, 'author_id': author_id})

            cnx.execute(text("UPDATE messages_count SET reaction = reaction + :d WHERE author_id =:r_user_id"),
                        {'d': delta, 'r_user_id': user_id})
            cnx.execute(text("UPDATE messages_count SET reacted = reacted + :d WHERE author_id =:id"),
                        {'d': delta, 'id': author_id})

    @bot.event
    async def on_raw_reaction_add(payload):
        if payload.guild_id is None or payload.user_id == bot.user.id:
            return
        if payload.member is not None and payload.member.bot:
            return
        author_id = await resolve_message_author(payload)
        if author_id is None:
            return
        await run_db(store_reaction, payload.message_id, payload.emoji.id, payload.user_id, author_id, 1)

    @bot.event
    async def on_raw_reaction_remove(payload):
        if payload.guild_id is None or payload.user_id == bot.user.id:
            return
        author_id = await resolve_message_author(payload)
        if author_id is None:
            return
        await run_db(store_reaction, payload.message_id, payload.emoji.id, payload.user_id, author_id, -1)

        user = bot.get_user(payload.user_id)
        user_name = discord.utils.escape_markdown(user.name) if user else f"<@{payload.user_id}>"

        jump_url = f"https://discord.com/channels/{payload.guild_id}/{payload.channel_id}/{payload.message_id}"
        await send_log(f"Usunięta reakcja {payload.emoji} użytkownika **{user_name}** do wiadomości {jump_url}")

    @bot.command(name='toxic')
    async def count_toxic_messages(ctx, user: discord.User = False):
        if ctx.channel.id != int(os.getenv('COMMAND_CHANNEL_ID')):
            return
        if not user:
            user = ctx.author
        if user.id not in bots:
            with bot_engine.connect() as cnx:
                row = cnx.execute(
                    text("SELECT all_24, toxic_24 FROM messages_count WHERE author_id = :id"),
                    {'id': user.id}
                ).fetchone()
            if row is None:
                await ctx.reply(f"Użytkownik **{user.name}** ma mniej niż 100 wiadomości na serwerze")
                return
            total_count, toxic_count = row[0] or 0, row[1] or 0

            if total_count > 100:
                if toxic_count > 10:
                    await ctx.reply(
                        f"Poziom toksyczności użytkownika **{user.name}** wynosi {round(toxic_count / total_count * 100, 3)}%.")
                else:
                    await ctx.reply(f"Użytkownik **{user.name}** ma mniej niż 10 toksycznych wiadomości na serwerze")
            else:
                await ctx.reply(f"Użytkownik **{user.name}** ma mniej niż 100 wiadomości na serwerze")

    @count_toxic_messages.error
    async def info_error(ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.reply('Użytkownika nie ma na serwerze')

    @bot.command(name='avatar')
    async def get_avatar(ctx, user: discord.User = False):
        if not user:
            user = ctx.author
        av = user.display_avatar.url
        await ctx.reply(content=f'Avatar użytkownika **{user.name}**', embed=discord.Embed().set_image(url=av))

    @bot.command(name='help')
    async def get_help(ctx):
        final = ('### Kanał https://discord.com/channels/733758693971066960/985565935849070702:\n'
                 '**!best [@user]** - wyświetla wiadomość z największą liczbą pojedynczej reakcji\n'
                 '**!bestall [@user]** - wyświetla wiadomość z największą liczbą wszystkich reakcji\n'
                 '**!first [@user]** - wyświetla pierwszą wiadomość użytkownika\n'
                 '**!messages** - ranking liczby wiadomości na serwerze (TOP10)\n'
                 '**!reactions** - ranking liczby dodanych reakcji (TOP10)\n'
                 '**!reacted** - ranking liczby otrzymanych reakcji (TOP10)\n'
                 '**!stats [@user]** - statystyki użytkownika - liczba wiadomości oraz dodanych i otrzymanych reakcji\n'
                 '**!toxic [@user]** - wyświetla poziom toksyczności użytkownika (procent wiadomości, '
                 'użytkownik musi mieć minimum 100 wiadomości i 10 toksycznych wiadomości na serwerze)\n\n'
                 '### Wszystkie kanały:\n'
                 '**!avatar [@user]** - wyświetla avatar użytkownika\n'
                 '**!czystobylo** - losowy gif z serii "czysto było"\n'
                 '**!help** - zbiór dostępnych komend\n'
                 '**!facebook | fb** - link do Facebooka\n'
                 '**!twitch | tt** - link do Twitcha\n'
                 '**!youtube | yt** - link do YouTube\n'
                 '**!twitter | x** - link do X/Twittera\n'
                 '**!instagram | ig** - link do Instagrama\n'
                 '**!formularz-f1** - formularz zgłoszeniowy incydentów F1\n'
                 '**!formularz-lmu** - formularz zgłoszeniowy incydentów LMU\n\n'
                 '### Opcje:\n'
                 '**[]** - opcjonalny argument\n'
                 '**|** - alias')

        embed = discord.Embed(
            colour=discord.Colour.dark_green(),
            title='Dostępne komendy',
            description=final
        )
        await ctx.reply(embed=embed)

    @bot.command(name='facebook', aliases=['fb'])
    async def get_fb(ctx):
        await ctx.reply('https://www.facebook.com/SimSprintSeries')

    @bot.command(name='twitch', aliases=['tt'])
    async def get_tt(ctx):
        await ctx.reply('https://www.twitch.tv/SimSprintSeries')

    @bot.command(name='youtube', aliases=['yt'])
    async def get_yt(ctx):
        await ctx.reply('https://www.youtube.com/SimSprintSeries')

    @bot.command(name='twitter', aliases=['x'])
    async def get_x(ctx):
        await ctx.reply('https://www.x.com/SimSprintSeries')

    @bot.command(name='instagram', aliases=['ig'])
    async def get_ig(ctx):
        await ctx.reply('https://www.instagram.com/SimSprintSeries')

    @bot.command(name='formularz-f1')
    async def get_ig(ctx):
        await ctx.reply('https://www.simss.pl/incydenty')

    @bot.command(name='formularz-lmu')
    async def get_ig(ctx):
        await ctx.reply('https://forms.gle/9NMeUaAKuD8qxc5M7')

    @bot.command(name='first')
    async def get_first_message(ctx, user: discord.User = False):
        if ctx.channel.id != int(os.getenv('COMMAND_CHANNEL_ID')):
            return
        if not user:
            user = ctx.author
        with bot_engine.connect() as cnx:
            row = cnx.execute(text(
                """SELECT message_id FROM messages m LEFT JOIN channels c ON m.channel_id = c.id
               WHERE m.author_id = :id
               AND m.type NOT LIKE 'GuildMemberJoin'
               AND c.is_active = 1
               ORDER BY DATE(m.timestamp) LIMIT 1"""), {'id': user.id}).fetchone()

        try:
            message_id = row[0]
            with bot_engine.connect() as cnx:
                channel_id = cnx.execute(
                    text("SELECT channel_id FROM messages WHERE message_id = :mid"),
                    {'mid': message_id}
                ).fetchone()[0]
            await ctx.reply(f"Pierwsza wiadomość użytkownika **{user.name}**: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik **{user.name}** nie napisał żadnej wiadomości na tym serwerze")

    @get_first_message.error
    async def info_error(ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.reply('Użytkownika nie ma na serwerze')

    @bot.command(name='best')
    async def get_best_message(ctx, user: discord.User = False):
        if ctx.channel.id != int(os.getenv('COMMAND_CHANNEL_ID')):
            return
        if not user:
            user = ctx.author
        with bot_engine.connect() as cnx:
            row = cnx.execute(text(
                "SELECT r.message_id FROM reactions r "
                "LEFT JOIN messages m ON r.message_id = m.message_id "
                "LEFT JOIN channels c ON m.channel_id = c.id "
                "WHERE r.author_id = :id AND c.is_active = 1 AND r.reaction_id != '' "
                "GROUP BY r.message_id, r.reaction_id ORDER BY COUNT(*) DESC LIMIT 1"
            ), {'id': user.id}).fetchone()

        try:
            message_id = row[0]
            with bot_engine.connect() as cnx:
                channel_id = cnx.execute(
                    text("SELECT channel_id FROM messages WHERE message_id = :mid"),
                    {'mid': message_id}
                ).fetchone()[0]
            await ctx.reply(f"Wiadomość użytkownika **{user.name}** z największą liczbą jednej reakcji: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik **{user.name}** nie napisał żadnej wiadomości na tym serwerze")

    @get_best_message.error
    async def info_error(ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.reply('Użytkownika nie ma na serwerze')

    @bot.command(name='bestall')
    async def get_best_message_all(ctx, user: discord.User = False):
        if ctx.channel.id != int(os.getenv('COMMAND_CHANNEL_ID')):
            return
        if not user:
            user = ctx.author
        with bot_engine.connect() as cnx:
            row = cnx.execute(text(
                "SELECT r.message_id FROM reactions r "
                "LEFT JOIN messages m ON r.message_id = m.message_id "
                "LEFT JOIN channels c ON m.channel_id = c.id "
                "WHERE r.author_id = :id AND c.is_active = 1 "
                "GROUP BY r.message_id ORDER BY COUNT(*) DESC LIMIT 1"
            ), {'id': user.id}).fetchone()

        try:
            message_id = row[0]
            with bot_engine.connect() as cnx:
                channel_id = cnx.execute(
                    text("SELECT channel_id FROM messages WHERE message_id = :mid"),
                    {'mid': message_id}
                ).fetchone()[0]
            await ctx.reply(f"Wiadomość użytkownika **{user.name}** z największą liczbą wszystkich reakcji: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik **{user.name}** nie napisał żadnej wiadomości na tym serwerze")

    @get_best_message_all.error
    async def info_error(ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.reply('Użytkownika nie ma na serwerze')

    @bot.command(name='czystobylo')
    async def czystobylo(message):
        await message.reply(random.choice(czysto))

    @bot.command(name='stats')
    async def get_stats(ctx, user: discord.User = False):
        if ctx.channel.id != int(os.getenv('COMMAND_CHANNEL_ID')):
            return
        if not user:
            user = ctx.author
        with bot_engine.connect() as cnx:
            result = cnx.execute(text(
                "SELECT * FROM (SELECT `all`, reaction, reacted, reacted/`all`, RANK() OVER(ORDER BY `all` DESC) AS rank_all, "
                "RANK() OVER(ORDER BY reaction desc) AS rank_reactions, RANK() OVER(ORDER BY reacted DESC) "
                "AS rank_reacted, RANK() OVER(ORDER BY reacted/`all` DESC) AS ratio, author_id FROM messages_count) "
                "AS t WHERE author_id = :id"
            ), {'id': user.id}).fetchone()

        if result is None:
            await ctx.reply(f"Brak statystyk dla użytkownika **{user.name}**")
            return

        final = (f"Liczba wiadomości: {result[0]} ({result[4]}.)\n"
                 f"Liczba dodanych reakcji: {result[1]} ({result[5]}.)\n"
                 f"Liczba otrzymanych reakcji: {result[2]} ({result[6]}.)\n"
                 f"Ratio otrzymane reakcje:wiadomości: {result[3]} ({result[7]}.)")
        embed = discord.Embed(
            colour=discord.Colour.dark_green(),
            title=f'Statystyki użytkownika {user.name}',
            description=final
        )
        await ctx.reply(embed=embed)

    @get_stats.error
    async def info_error(ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.reply('Użytkownika nie ma na serwerze')

    @bot.command(name='messages')
    async def get_stats(ctx):
        if ctx.channel.id != int(os.getenv('COMMAND_CHANNEL_ID')):
            return
        with bot_engine.connect() as cnx:
            result = cnx.execute(text(
                "SELECT author_id, name, `all` FROM messages_count mc "
                "LEFT JOIN members m ON mc.author_id = m.id "
                "ORDER BY `all` DESC LIMIT 10"
            )).fetchall()
        final = ''
        for i in range(len(result)):
            final += str(i) + '. ' + str(result[i][1]) + ': ' + str(result[i][2]) + '\n'
        embed = discord.Embed(
            colour=discord.Colour.dark_green(),
            title='Najwięcej wiadomości na serwerze (tylko otwarte i istniejące kanały)',
            description=final
        )
        await ctx.reply(embed=embed)

    @bot.command(name='reactions')
    async def get_reactions(ctx):
        if ctx.channel.id != int(os.getenv('COMMAND_CHANNEL_ID')):
            return
        with bot_engine.connect() as cnx:
            result = cnx.execute(text(
                "SELECT author_id, name, reaction FROM messages_count mc "
                "LEFT JOIN members m ON mc.author_id = m.id "
                "ORDER BY reaction DESC LIMIT 10"
            )).fetchall()
        final = ''
        for i in range(len(result)):
            final += str(i) + '. ' + str(result[i][1]) + ': ' + str(result[i][2]) + '\n'
        embed = discord.Embed(
            colour=discord.Colour.dark_green(),
            title='Najwięcej dodanych reakcji na serwerze (tylko otwarte i istniejące kanały)',
            description=final
        )
        await ctx.reply(embed=embed)

    @bot.command(name='reacted')
    async def get_reacted(ctx):
        if ctx.channel.id != int(os.getenv('COMMAND_CHANNEL_ID')):
            return
        with bot_engine.connect() as cnx:
            result = cnx.execute(text(
                "SELECT author_id, name, reacted FROM messages_count mc "
                "LEFT JOIN members m ON mc.author_id = m.id "
                "ORDER BY reacted DESC LIMIT 10"
            )).fetchall()
        final = ''
        for i in range(len(result)):
            final += str(i) + '. ' + str(result[i][1]) + ': ' + str(result[i][2]) + '\n'
        embed = discord.Embed(
            colour=discord.Colour.dark_green(),
            title='Najwięcej otrzymanych reakcji na serwerze (tylko otwarte i istniejące kanały)',
            description=final
        )
        await ctx.reply(embed=embed)

    # ---------------------- Generate Embed ----------------------
    def build_embed(event_id):
        with garage_engine.connect() as cnx:
            event = cnx.execute(text("""SELECT e.id, e.name, l.id, l.name
                FROM event e
                LEFT JOIN league l ON e.league_id = l.id
                WHERE e.id = :event_id AND e.deleted <> 1;"""), {'event_id': event_id}).fetchone()

            if event is None:
                return None

            rows = cnx.execute(text("""SELECT p.is_present, du.id, t.name, t.colour,
                IF(dl.id IS NOT NULL AND t.id IS NOT NULL, 1, 0) as isAssigned, l.id, t.team_emoji
                FROM presence p
                LEFT JOIN event e ON p.event_id = e.id
                LEFT JOIN league l ON e.league_id = l.id
                LEFT JOIN driver d ON p.driver_id = d.id
                LEFT JOIN discord_user du ON d.discord_user_id = du.id
                LEFT JOIN driver_leagues dl ON dl.drivers_id = d.id AND dl.leagues_id = l.id AND dl.deleted <> 1
                LEFT JOIN team t ON dl.team_id = t.id AND t.deleted <> 1 AND t.name <> 'Rezerwa'
                WHERE p.event_id = :event_id AND p.deleted <> 1
                ORDER BY p.date;"""), {'event_id': event_id}).fetchall()

            rows = [r for r in rows if r[1] is not None]

            league_ids = sorted({r[5] for r in rows if r[5] is not None})
            if not league_ids:
                teams = []
            else:
                teams = cnx.execute(text("SELECT t.name, t.team_emoji FROM team t "
                                         "LEFT JOIN game g ON t.game_id = g.id AND g.dtype = 'Game' "
                                         "LEFT JOIN game gf ON g.game_family_id = gf.id AND gf.dtype = 'GameFamily' "
                                         "LEFT JOIN league l ON g.id = l.game_id "
                                         "WHERE l.id IN :league_ids AND t.name <> 'Rezerwa' "
                                         "AND gf.name = 'F1'").bindparams(bindparam('league_ids', expanding=True)),
                                    {'league_ids': league_ids}).fetchall()

        embed = discord.Embed(
            title=f"{event[1]} - {event[3]}",
            description=f"Zgłoś obecność lub nieobecność na stronie {os.getenv('DOMAIN_NAME')}/season/{event[2]}/event/{event[0]}",
            color=discord.Color.green(),
        )

        guild = bot.get_guild(int(os.getenv('SERVER_ID')))

        if teams:
            for t in teams:
                field = [name_or_mention(guild, r[1]) for r in rows if r[0] == 1 and r[4] == 1 and r[2] == t[0]]
                embed.add_field(name=f"{t[1]} {t[0]} ({len(field)})", value=field_value(field) or "—", inline=True)
        else:
            field = [name_or_mention(guild, r[1]) for r in rows if r[0] == 1 and r[4] == 1]
            embed.add_field(name=f"✅ Obecność ({len(field)})", value=field_value(field) or "—", inline=True)
        declined = [name_or_mention(guild, r[1]) for r in rows if r[0] == 0 and r[4] == 1]
        embed.add_field(name=f"❌ Nieobecność ({len(declined)})", value=field_value(declined) or "—", inline=True)
        reserve = [name_or_mention(guild, r[1]) for r in rows if r[0] == 1 and r[4] == 0]
        embed.add_field(name=f"🟣 Rezerwa ({len(reserve)})", value=field_value(reserve) or "—", inline=True)

        return embed

    # ---------------------- Trap channel helpers ----------------------
    async def purge_user_messages(guild, member, cutoff):
        async def purge_channel(channel):
            perms = channel.permissions_for(member)
            if not perms.view_channel:
                return []
            try:
                return await channel.purge(
                    limit=200,
                    after=cutoff,
                    check=lambda m: m.author.id == member.id,
                    bulk=True,
                )
            except (discord.Forbidden, discord.HTTPException):
                return []

        await asyncio.gather(*(purge_channel(ch) for ch in guild.text_channels))

    async def handle_trap(message):
        member = message.author

        if member.id in trapped_users:
            return

        trapped_users.add(member.id)
        timeout_minutes = int(os.getenv('TIMEOUT_MINUTES'))
        delete_minutes = int(os.getenv('DELETE_MESSAGES_MINUTES'))
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=delete_minutes)
        try:
            await member.timeout(timedelta(minutes=timeout_minutes), reason="Wiadomość na kanale-pułapce (spam)")
        except discord.Forbidden:
            log.warning("Nie można wyciszyć %s - brak uprawnień", member.id)

        async def purge_and_release():
            try:
                await purge_user_messages(message.guild, member, cutoff)
            finally:
                trapped_users.discard(member.id)

            asyncio.create_task(purge_and_release())
            asyncio.create_task(send_warning_dm(member, timeout_minutes))

    async def send_warning_dm(member, timeout_minutes):
        try:
            await member.send(
                f"Zostałeś wyciszony na {timeout_minutes} minut na serwerze ze względu na spam. "
                f"Natychmiast zmień hasło i wymuś wylogowanie ze wszystkich urządzeń.")
        except (discord.Forbidden, discord.HTTPException):
            pass

        # ---------------------- Scheduled Task ----------------------

    def fetch_due_events():
        with garage_engine.connect() as cnx:
            return cnx.execute(text("""
                SELECT dbe.id, e.id, gf.send_post_channel_id, l.discord_group_id, dbe.message_id FROM discord_bot_events dbe
                LEFT JOIN event e ON dbe.event_id = e.id
                LEFT JOIN league l ON e.league_id = l.id
                LEFT JOIN game g ON g.id = l.game_id AND g.dtype = 'Game'
                LEFT JOIN game gf ON gf.id = g.game_family_id AND gf.dtype = 'GameFamily'
                WHERE e.start_date >= NOW()
                AND DATE_SUB(e.start_date, INTERVAL l.send_post_hours HOUR) <= NOW();
            """)).fetchall()

    def save_event_message(dbe_id, message_id):
        with garage_engine.begin() as cnx:
            cnx.execute(text("UPDATE discord_bot_events SET message_id =:message_id WHERE id =:id"),
                        {'message_id': message_id, 'id': dbe_id})

    missing_event_messages = set()

    async def process_event(event):
        dbe_id, event_id, channel_id, role_id, message_id = event
        channel = bot.get_channel(int(channel_id)) if channel_id else None
        if channel is None:
            log.warning("Event %s: brak kanału %s", event_id, channel_id)
            return
        embed = build_embed(event_id)
        if embed is None:
            return
        if message_id is None:
            message = await channel.send(content=f"<@&{role_id}>", embed=embed,
                                         allowed_mentions=discord.AllowedMentions(roles=True))
            await run_db(save_event_message, dbe_id, message.id)
            return
        try:
            message = await channel.fetch_message(int(message_id))
            await message.edit(embed=embed)
        except discord.NotFound:
            if message_id not in missing_event_messages:
                missing_event_messages.add(message_id)
                log.warning("Event %s: wiadomość %s została usunięta", event_id, message_id)

    @tasks.loop(minutes=1)
    async def event_checker():
        try:
            events = await run_db(fetch_due_events)
        except Exception:
            log.exception("event_checker: błąd pobierania eventów")
            return
        for event in events:
            try:
                await process_event(event)
            except Exception:
                log.exception("event_checker: błąd przy evencie %s", event[1])

    async def schedule_reminders():
        with garage_engine.connect() as cnx:
            reminders = cnx.execute(text("""
                SELECT e.id, e.name, l.id, l.name, emb.value, DATE_SUB(e.start_date, INTERVAL emb.value HOUR) AS reminder_time
                FROM discord_bot_events dbe
                LEFT JOIN event e on dbe.event_id = e.id
                LEFT JOIN league l on e.league_id = l.id
                RIGHT JOIN event_mention_before emb on l.id = emb.league_id
                WHERE DATE_SUB(e.start_date, INTERVAL emb.value HOUR) >= NOW();
            """)).fetchall()

        for r in reminders:
            try:
                scheduler.add_job(
                    send_reminder,
                    "date",
                    run_date=r[5],
                    args=[r[0], r[1], r[2], r[3], r[4]],
                    id=f"reminder_{r[0]}_{r[4]}",
                    replace_existing=True,
                )
            except Exception:
                log.exception("event_checker: błąd przy evencie %s", r[0])


    async def send_reminder(event_id, event_name, league_id, league_name, hours_before):
        with garage_engine.connect() as cnx:
            not_selected_rows = cnx.execute(text("""SELECT du.id FROM driver_leagues dl
                INNER JOIN team t ON dl.team_id = t.id AND t.deleted <> 1 AND t.name <> 'Rezerwa'
                INNER JOIN driver d ON dl.drivers_id = d.id
                INNER JOIN discord_user du ON d.discord_user_id = du.id
                WHERE dl.leagues_id = :league_id AND d.id NOT IN
                (SELECT d.id FROM presence p
                LEFT JOIN event e ON p.event_id = e.id
                LEFT JOIN driver d ON p.driver_id = d.id
                LEFT JOIN league l ON e.league_id = l.id
                LEFT JOIN driver_leagues dl ON dl.drivers_id = d.id AND dl.leagues_id = l.id
                INNER JOIN team t ON dl.team_id = t.id AND t.deleted <> 1 AND t.name <> 'Rezerwa'
                WHERE e.id = :event_id AND p.deleted <> 1);"""), {'league_id': league_id, 'event_id': event_id}).fetchall()

        not_selected = (x[0] for x in not_selected_rows)

        msg = (f"{event_name} w lidze {league_name} rozpoczyna się za {hours_text(hours_before)}. Zgłoś obecność lub nieobecność "
               f"na stronie: {os.getenv('DOMAIN_NAME')}/season/{league_id}/event/{event_id}")
        for (discord_id,) in not_selected_rows:
            if discord_id is None:
                continue
            user = bot.get_user(int(discord_id))
            try:
                if user is None:
                    user = await bot.fetch_user(int(discord_id))
                await user.send(msg)
            except discord.NotFound:
                log.info("Przypomnienie: nie znaleziono użytkownika %s", discord_id)
            except discord.Forbidden:
                log.info("Przypomnienie: użytkownik %s ma zamknięte DM-y", discord_id)
            except discord.HTTPException as e:
                log.warning("Przypomnienie: błąd wysyłki do %s: %s", discord_id, e)

    def hours_text(hours):
        if hours == 1:
            return '1 godzinę'
        elif 2 <= hours % 10 <= 4 and not 12 <= hours % 100 <= 14:
            return f"{hours} godziny"
        return f"{hours} godzin"

    bot.run(token)
