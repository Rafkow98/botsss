import asyncio
import random
import re

import gspread
import pandas as pd

import discord
from discord import MessageType
from discord.ext import commands, tasks
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, text

def authenticate_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

    client = gspread.authorize(creds)
    return client


def run_bot(bot_connection_string, garage_connection_string, token):
    intents = discord.Intents.all()
    intents.members = True

    bot = commands.Bot(command_prefix="!", intents=intents)
    bot.remove_command("help")

    engine = create_engine(bot_connection_string)
    connection = engine.raw_connection()
    cursor = connection.cursor()

    bots = [968851237405597717, 475744554910351370, 235148962103951360, 762217899355013120, 159985870458322944,
            1311665961027244084, 734535151899639910, 1311665961027244084, 1211781489931452447]

    command_channels = [985565935849070702, 799608563566772234, 857643204958879797]

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

    invite_pattern = re.compile(r"discord\.gg\/[A-Za-z0-9]+|discord\.com\/invite\/[A-Za-z0-9]+")

    admins = [523929325171638280, 341537077474885632, 917064080366391386, 686636820196491305, 258707097036914689, 250367142073991178]

    async def check_for_new_responses():
        google_client = authenticate_google_sheets()
        f1_sheet = google_client.open('Formularz zgłoszeniowy incydentów wyścigowych SSS (Odpowiedzi)').sheet1
        f1_last_processed_row = len(f1_sheet.get_all_records())
        acc_sheet = google_client.open('Incydenty ACC/LMU').get_worksheet(1)
        acc_last_processed_row = len(acc_sheet.get_all_records())
        lmu_sheet = google_client.open('Incydenty ACC/LMU').get_worksheet(0)
        lmu_last_processed_row = len(lmu_sheet.get_all_records())
        clips_sheet = google_client.open('Klipy (Odpowiedzi)').get_worksheet(0)
        clips_last_processed_row = len(clips_sheet.get_all_records())

        while True:
            f1_responses = f1_sheet.get_all_records()
            f1_new_responses = f1_responses[f1_last_processed_row:]
            f1_last_processed_row = len(f1_responses)

            if f1_new_responses:
                for response in f1_new_responses:
                    final = (f"Zgłaszający: {response['Zgłaszający kierowca']}\n"
                             f"Zgłaszany: {response['Zgłaszany kierowca']}\n"
                             f"Wyścig: {response['Wyścig']}\n"
                             f"Split: {response['Split']}\n"
                             f"Numer okrążenia: {response['Numer okrążenia']}\n"
                             f"Dowód: {response['Dowód']}\n"
                             f"Opis incydentu: {response['Opis incydentu']}")
                    embed = discord.Embed(
                        colour=discord.Colour.dark_green(),
                        title=f'Zgłoszenie {str(f1_last_processed_row + 1)}',
                        description=final
                    )
                    channel = bot.get_channel(1015386642485362744)
                    await channel.send(embed=embed)

            acc_responses = acc_sheet.get_all_records()
            acc_new_responses = acc_responses[acc_last_processed_row:]
            acc_last_processed_row = len(acc_responses)

            if acc_new_responses:
                for response in acc_new_responses:
                    final = (f"Zgłaszający: {response['Kierowca zgłaszający / nr auta']}\n"
                             f"Zgłaszany: {response['Kierowca zgłaszany / numer auta']}\n"
                             f"Wyścig: {response['Wybierz rundę']}\n"
                             f"Rodzaj zdarzenia: {response['Rodzaj zdarzenia']}\n"
                             f"Dowód (link/timestamp): {response['Dowody ( Link do nagrania z incydentu/sygnatura czasowa z oficjalnej powtórki)']}\n"
                             f"Opis incydentu: {response['Opis sytuacji']}")
                    embed = discord.Embed(
                        colour=discord.Colour.dark_green(),
                        title=f'Zgłoszenie {str(acc_last_processed_row + 1)}',
                        description=final
                    )
                    channel = bot.get_channel(1366514242051903650)
                    await channel.send(embed=embed)

            lmu_responses = lmu_sheet.get_all_records()
            lmu_new_responses = lmu_responses[lmu_last_processed_row:]
            lmu_last_processed_row = len(lmu_responses)

            if lmu_new_responses:
                for response in lmu_new_responses:
                    final = (f"Zgłaszający: {response['Kierowca zgłaszający / nr auta']}\n"
                             f"Zgłaszany: {response['Kierowca zgłaszany / numer auta']}\n"
                             f"Wyścig: {response['Wybierz rundę']}\n"
                             f"Split: {response['Split']}\n"
                             f"Klasa: {response['Klasa Auta']}\n"
                             f"Rodzaj zdarzenia: {response['Rodzaj zdarzenia']}\n"
                             f"Dowód (link/timestamp): {response['Dowody ( Link do nagrania z incydentu)']}\n"
                             f"Opis incydentu: {response['Opis sytuacji']}")
                    embed = discord.Embed(
                        colour=discord.Colour.dark_green(),
                        title=f'Zgłoszenie {str(lmu_last_processed_row + 1)}',
                        description=final
                    )
                    channel = bot.get_channel(1334193229116997703)
                    await channel.send(embed=embed)

            clips_responses = clips_sheet.get_all_records()
            clips_new_responses = clips_responses[clips_last_processed_row:]
            clips_last_processed_row = len(clips_responses)

            if clips_new_responses:
                for response in clips_new_responses:
                    final = (f"Nick: {response['Nick']}\n"
                             f"Split: {response['Split']}\n"
                             f"Wyścig: {response['Wyścig']}\n"
                             f"Wideo: {response['Wideo']}")
                    embed = discord.Embed(
                        colour=discord.Colour.dark_green(),
                        title=f'Klip {str(clips_last_processed_row + 1)}',
                        description=final
                    )
                    channel = bot.get_channel(1062362633933692968)
                    role = discord.utils.get(channel.guild.roles, id=1062361963277070346)
                    await channel.send(role.mention, embed=embed)

            await asyncio.sleep(10)

    @bot.event
    async def on_ready():
        event_checker.start()
        channels_list = []
        members_list = []
        emojis_list = []
        for guild in bot.guilds:
            for channel in guild.text_channels:
                channels_list.append({'id': channel.id, 'name': channel.name})
            for member in guild.members:
                members_list.append({'id': member.id, 'name': member.name, 'avatar': member.avatar})
            for emoji in guild.emojis:
                emojis_list.append({'id': emoji.id, 'name': emoji.name})

        channels = pd.DataFrame(channels_list)
        channels.to_sql(name='channels', con=engine, if_exists='replace', index=False)

        members = pd.DataFrame(members_list)
        members.to_sql(name='members', con=engine, if_exists='replace', index=False)

        emojis = pd.DataFrame(emojis_list)
        emojis.to_sql(name='emojis', con=engine, if_exists='replace', index=False)

        with engine.begin() as cnx:
            cnx.execute(text("UPDATE messages SET is_active_channel = "
                             "CASE WHEN channel_id IN (SELECT id FROM channels) THEN 1 "
                             "ELSE 0 END"))

        await check_for_new_responses()

    @bot.event
    async def on_member_join(member):
        with engine.begin() as cnx:
            cnx.execute(text("INSERT INTO members(id, name) VALUES(:id, :name)"),
                        {'id': member.id, 'name': member.name})

    @bot.event
    async def on_member_remove(member):
        with engine.begin() as cnx:
            cnx.execute(text("DELETE FROM members WHERE id = :id"), {'id': member.id})

        if member.id == 258707097036914689:
            channel = await bot.fetch_channel(734136364689260604)
            await channel.send(file=discord.File("IMG_9668.png", filename="IMG_9668.png"))

    @bot.event
    async def on_message(message):
        if bot.user.mentioned_in(message) and message.type != MessageType.reply:
            await message.reply(random.choice(gifs))
        if message.channel.id == 1365264124509945907 and len(message.attachments) == 0:
            await message.delete()
        with engine.begin() as cnx:
            cnx.execute(text("INSERT INTO messages(id, type, timestamp, timestampEdited, isPinned, content, author_id, "
                             "channel_id, attachments, embeds, stickers, mentions) "
                             "VALUES(:id, :type, :created, :edited, :pinned, :content, :author, :channel, :attachments, :embeds, :stickers, :mentions)"),
                        {'id': message.id, 'type': message.type, 'created': message.created_at,
                         'edited': message.edited_at,
                         'pinned': message.pinned, 'content': message.content, 'author': message.author.id,
                         'channel': message.channel.id,
                         'attachments': bool(message.attachments), 'embeds': bool(message.embeds),
                         'stickers': bool(message.stickers),
                         'mentions': bool(message.mentions)})
        is_toxic = any(elem in message.content.lower() for elem in toxic_words)
        is_racist = any(elem in message.content.lower() for elem in racist_words)
        if is_toxic and is_racist:
            with engine.begin() as cnx:
                cnx.execute(text("UPDATE messages_count SET `all` = `all` + 1, all_24 = all_24 + 1, toxic = toxic + 1, "
                                 "toxic_24 = toxic_24 + 1, racism = racism + 1, racism_24 = racism_24 + 1 WHERE author_id = :id"),
                            {'id': message.author.id})
        elif is_toxic and not is_racist:
            with engine.begin() as cnx:
                cnx.execute(text("UPDATE messages_count SET `all` = `all` + 1, all_24 = all_24 + 1, toxic = toxic + 1, "
                                 "toxic_24 = toxic_24 + 1 WHERE author_id = :id"),
                            {'id': message.author.id})
        elif not is_toxic and is_racist:
            with engine.begin() as cnx:
                cnx.execute(
                    text("UPDATE messages_count SET `all` = `all` + 1, all_24 = all_24 + 1, racism = racism + 1, "
                         "racism_24 = racism_24 + 1 WHERE author_id = :id"),
                    {'id': message.author.id})
        else:
            with engine.begin() as cnx:
                cnx.execute(
                    text("UPDATE messages_count SET `all` = `all` + 1, all_24 = all_24 + 1 WHERE author_id = :id"),
                    {'id': message.author.id})
        if invite_pattern.search(message.content):
            if message.author.id not in admins:
                await message.delete()
                await message.channel.send(
                    f'{message.author.mention}, wysyłanie zaproszeń jest zabronione na tym serwerze')

        await bot.process_commands(message)

    @bot.event
    async def on_message_edit(before, after):
        if before.author.id not in bots and after.embeds == []:
            with engine.begin() as cnx:
                cnx.execute(text("UPDATE messages SET content = :new, attachments = :attachments, embeds = :embeds, "
                                 "stickers = :stickers, mentions = :mentions WHERE id = :id"),
                            {'new': after.content, 'attachments': bool(after.attachments), 'embeds': bool(after.embeds),
                             'stickers': bool(after.stickers), 'mentions': bool(after.mentions), 'id': after.id})

            bin_channel = bot.get_channel(734535036338176021)
            await bin_channel.send(f"*Edytowana wiadomość użytkownika **{before.author.name}** *\n"
                                   f"Stara wersja: {before.content}\n"
                                   f"Nowa wersja: {after.content}\n"
                                   f"Wiadomość: {after.jump_url}")

    @bot.event
    async def on_message_delete(message):
        if message.author.id not in bots:
            with engine.begin() as cnx:
                cnx.execute(text("DELETE FROM messages WHERE id = :id"), {'id': message.id})
        is_toxic = any(elem in message.content.lower() for elem in toxic_words)
        is_racist = any(elem in message.content.lower() for elem in racist_words)
        if is_toxic and is_racist:
            with engine.begin() as cnx:
                cnx.execute(text("UPDATE messages_count SET `all` = `all` - 1, all_24 = all_24 - 1, toxic = toxic - 1, "
                                 "toxic_24 = toxic_24 - 1, racism = racism - 1, racism_24 = racism_24 - 1 WHERE author_id = :id"),
                            {'id': message.author.id})
        elif is_toxic and not is_racist:
            with engine.begin() as cnx:
                cnx.execute(text("UPDATE messages_count SET `all` = `all` - 1, all_24 = all_24 - 1, toxic = toxic - 1, "
                                 "toxic_24 = toxic_24 - 1 WHERE author_id = :id"),
                            {'id': message.author.id})
        elif not is_toxic and is_racist:
            with engine.begin() as cnx:
                cnx.execute(
                    text("UPDATE messages_count SET `all` = `all` - 1, all_24 = all_24 - 1, racism = racism - 1, "
                         "racism_24 = racism_24 - 1 WHERE author_id = :id"),
                    {'id': message.author.id})
        else:
            with engine.begin() as cnx:
                cnx.execute(
                    text("UPDATE messages_count SET `all` = `all` - 1, all_24 = all_24 - 1 WHERE author_id = :id"),
                    {'id': message.author.id})

            bin_channel = bot.get_channel(734535036338176021)
            await bin_channel.send(f"*Usunięta wiadomość użytkownika **{message.author.name}** *\n{message.content}")

    @bot.event
    async def on_raw_reaction_add(payload):
        channel = bot.get_channel(payload.channel_id)
        message = await channel.fetch_message(payload.message_id)

        with engine.begin() as cnx:
            cnx.execute(text("INSERT INTO reactions(message_id, reaction_id, reacting_user_id, author_id) "
                             "VALUES(:message_id, :reaction_id, :r_user_id, :author_id)"),
                        {'message_id': payload.message_id, 'reaction_id': payload.emoji.id,
                         'r_user_id': payload.user_id,
                         'author_id': message.author.id})
            cnx.execute(text("UPDATE messages_count SET reaction = reaction + 1 WHERE author_id = :r_user_id"),
                        {'r_user_id': payload.user_id})
            cnx.execute(text("UPDATE messages_count SET reacted = reacted + 1 WHERE author_id = :id;"),
                        {'id': message.author.id})

    @bot.event
    async def on_raw_reaction_remove(payload):
        channel = bot.get_channel(payload.channel_id)
        message = await channel.fetch_message(payload.message_id)

        with engine.begin() as cnx:
            cnx.execute(text("DELETE FROM reactions WHERE message_id = :message_id AND reaction_id = :reaction_id "
                             "AND reacting_user_id = :r_user_id AND author_id = :author_id"),
                        {'message_id': payload.message_id, 'reaction_id': payload.emoji.id,
                         'r_user_id': payload.user_id,
                         'author_id': message.author.id})
            cnx.execute(text("UPDATE messages_count SET reaction = reaction - 1 WHERE author_id = :r_user_id"),
                        {'r_user_id': payload.user_id})
            cnx.execute(text("UPDATE messages_count SET reacted = reacted - 1 WHERE author_id = :id;"),
                        {'id': message.author.id})

        bin_channel = bot.get_channel(734535036338176021)
        await bin_channel.send(f"Usunięta reakcja <:{payload.emoji.name}:{payload.emoji.id}> użytkownika "
                               f"**{bot.get_user(payload.user_id).name}** do wiadomości {message.jump_url}")

    @bot.command(name='toxic')
    async def count_toxic_messages(ctx, user: discord.User = False):
        if ctx.channel.id not in command_channels:
            return
        if not user:
            user = ctx.author
        if user.id not in bots:
            cursor.execute("SELECT all_24 FROM messages_count WHERE author_id = " + str(user.id))
            total_count = cursor.fetchone()[0]
            cursor.execute("SELECT toxic_24 FROM messages_count WHERE author_id = " + str(user.id))
            toxic_count = cursor.fetchone()[0]
            connection.commit()

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
        av = user.avatar.url
        await ctx.reply(content=f'Avatar użytkownika **{user.name}**', embed=discord.Embed().set_image(url=av))

    @bot.command(name='help')
    async def get_help(ctx):
        final = ('### Kanał https://discord.com/channels/733758693971066960/985565935849070702:\n'
                 '**!avatar [@user]** - wyświetla avatar użytkownika\n'
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
                 '**!czystobylo** - losowy gif z serii czysto było\n'
                 '**!help** - zbiór dostępnych komend\n'
                 '**!facebook | fb** - link do Facebooka\n'
                 '**!twitch | tt** - link do Twitcha\n'
                 '**!youtube | yt** - link do YouTube\n\n'
                 '**!twitter | x** - link do X/Twittera\n\n'
                 '**!instagram | ig** - link do Instagrama\n\n'
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

    @bot.command(name='first')
    async def get_first_message(ctx, user: discord.User = False):
        if not user:
            user = ctx.author
        cursor.execute(
            "SELECT id FROM messages WHERE author_id = " + str(user.id) + " AND type NOT LIKE 'GuildMemberJoin' "
                                                                          "AND is_active_channel = 1 "
                                                                          "ORDER BY DATE(timestamp) LIMIT 1")
        try:
            message_id = cursor.fetchone()[0]
            cursor.execute("SELECT channel_id FROM messages WHERE id = " + str(message_id))
            channel_id = cursor.fetchone()[0]
            await ctx.reply(f"Pierwsza wiadomość użytkownika **{user.name}**: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik **{user.name}** nie napisał żadnej wiadomości na tym serwerze")
        connection.commit()

    @get_first_message.error
    async def info_error(ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.reply('Użytkownika nie ma na serwerze')

    @bot.command(name='best')
    async def get_best_message(ctx, user: discord.User = False):
        if not user:
            user = ctx.author
        cursor.execute("SELECT r.message_id FROM reactions r LEFT JOIN messages m ON r.message_id = m.id "
                       "WHERE r.author_id = " + str(user.id) + " AND m.is_active_channel = 1 AND r.reaction_id != '' "
                       "GROUP BY r.message_id, r.reaction_id ORDER BY COUNT(*) DESC LIMIT 1")
        try:
            message_id = cursor.fetchone()[0]
            cursor.execute("SELECT channel_id FROM messages WHERE id = " + str(message_id))
            channel_id = cursor.fetchone()[0]
            await ctx.reply(f"Wiadomość użytkownika **{user.name}** z największą liczbą jednej reakcji: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik **{user.name}** nie napisał żadnej wiadomości na tym serwerze")
        connection.commit()

    @get_best_message.error
    async def info_error(ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.reply('Użytkownika nie ma na serwerze')

    @bot.command(name='bestall')
    async def get_best_message_all(ctx, user: discord.User = False):
        if not user:
            user = ctx.author
        cursor.execute("SELECT r.message_id FROM reactions r LEFT JOIN messages m ON r.message_id = m.id "
                       "WHERE r.author_id = " + str(user.id) + " AND m.is_active_channel = 1 "
                       "GROUP BY r.message_id ORDER BY COUNT(*) DESC LIMIT 1")
        try:
            message_id = cursor.fetchone()[0]
            cursor.execute("SELECT channel_id FROM messages WHERE id = " + str(message_id))
            channel_id = cursor.fetchone()[0]
            await ctx.reply(f"Wiadomość użytkownika **{user.name}** z największą liczbą wszystkich reakcji: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik **{user.name}** nie napisał żadnej wiadomości na tym serwerze")
        connection.commit()

    @get_best_message_all.error
    async def info_error(ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.reply('Użytkownika nie ma na serwerze')

    @bot.command(name='czystobylo')
    async def czystobylo(message):
        await message.reply(random.choice(czysto))

    @bot.command(name='stats')
    async def get_stats(ctx, user: discord.User = False):
        if not user:
            user = ctx.author
        cursor.execute(
            "SELECT * FROM (SELECT `all`, reaction, reacted, reacted/`all`, rank() over(order by `all` desc) as rank_all, "
            "rank() over(order by reaction desc) as rank_reactions, rank() over(order by reacted desc) "
            "as rank_reacted, rank() over(order by reacted/`all` desc) as ratio, author_id FROM messages_count) "
            "as t WHERE author_id = " + str(user.id))
        result = cursor.fetchone()
        connection.commit()
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
        cursor.execute("SELECT author_id, `all` FROM messages_count ORDER BY `all` DESC LIMIT 10")
        result = cursor.fetchall()
        connection.commit()
        final = ''
        for i in range(len(result)):
            try:
                user = bot.get_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
            except AttributeError:
                try:
                    user = await bot.fetch_user(result[i][0])
                    final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
                except discord.errors.NotFound:
                    final += str(i) + '. usunięty użytkownik: ' + str(result[i][1]) + '\n'
        embed = discord.Embed(
            colour=discord.Colour.dark_green(),
            title='Najwięcej wiadomości na serwerze (tylko otwarte i istniejące kanały)',
            description=final
        )
        await ctx.reply(embed=embed)
        # await ctx.reply('Najwięcej wiadomości na serwerze (tylko otwarte i istniejące kanały):\n' + final)

    @bot.command(name='reactions')
    async def get_reactions(ctx):
        cursor.execute("SELECT author_id, reaction FROM messages_count ORDER BY reaction DESC LIMIT 10")
        result = cursor.fetchall()
        connection.commit()
        final = ''
        for i in range(len(result)):
            try:
                user = bot.get_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
            except AttributeError:
                try:
                    user = await bot.fetch_user(result[i][0])
                    final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
                except discord.errors.NotFound:
                    final += str(i) + '. usunięty użytkownik: ' + str(result[i][1]) + '\n'
        embed = discord.Embed(
            colour=discord.Colour.dark_green(),
            title='Najwięcej dodanych reakcji na serwerze (tylko otwarte i istniejące kanały)',
            description=final
        )
        await ctx.reply(embed=embed)

    @bot.command(name='reacted')
    async def get_reacted(ctx):
        cursor.execute("SELECT author_id, reacted FROM messages_count ORDER BY reacted DESC LIMIT 10")
        result = cursor.fetchall()
        connection.commit()
        final = ''
        for i in range(len(result)):
            try:
                user = bot.get_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
            except AttributeError:
                try:
                    user = await bot.fetch_user(result[i][0])
                    final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
                except discord.errors.NotFound:
                    final += str(i) + '. usunięty użytkownik: ' + str(result[i][1]) + '\n'
        embed = discord.Embed(
            colour=discord.Colour.dark_green(),
            title='Najwięcej otrzymanych reakcji na serwerze (tylko otwarte i istniejące kanały)',
            description=final
        )
        await ctx.reply(embed=embed)

    # ---------------------- Attendance Buttons ----------------------
    class AttendanceView(discord.ui.View):
        def __init__(self, event_id: int):
            super().__init__(timeout=None)
            self.discord_event_id = event_id

        @discord.ui.button(label="✓ Obecność", style=discord.ButtonStyle.success)
        async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
            await self.update_attendance(interaction, "accepted")

        @discord.ui.button(label="✗ Nieobecność", style=discord.ButtonStyle.danger)
        async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
            await self.update_attendance(interaction, "declined")

        @discord.ui.button(label="? Niepewność", style=discord.ButtonStyle.primary)
        async def tentative(self, interaction: discord.Interaction, button: discord.ui.Button):
            await self.update_attendance(interaction, "tentative")

        # --- Save selection and update embed ---
        async def update_attendance(self, interaction, status):
            garage_engine = create_engine(garage_connection_string)
            garage_connection = garage_engine.raw_connection()
            garage_cursor = garage_connection.cursor()

            garage_cursor.execute("""
                INSERT INTO presence (discord_bot_events_id, driver_id, status, timestamp)
                VALUES (%s, %s, %s, current_date)
                ON DUPLICATE KEY UPDATE status=%s
            """, (self.discord_event_id, interaction.user.id, status, status))

            garage_connection.commit()
            garage_cursor.close()
            garage_connection.close()

            await interaction.response.send_message(f"Zgłoszono **{status}**!", ephemeral=True)
            await update_event_embed(self.discord_event_id, interaction.message)

    # ---------------------- Generate Embed ----------------------
    def build_embed(discord_event_id):
        garage_engine = create_engine(garage_connection_string)
        garage_connection = garage_engine.raw_connection()
        garage_cursor = garage_connection.cursor()

        garage_cursor.execute("SELECT * FROM discord_bot_events WHERE id=%s", (discord_event_id,))
        event = garage_cursor.fetchone()

        garage_cursor.execute("SELECT * FROM presence WHERE discord_bot_events_id=%s", (discord_event_id,))
        rows = garage_cursor.fetchall()

        garage_connection.close()

        accepted = [f"<@{r[4]}>" for r in rows if r[2] == "accepted"]
        declined = [f"<@{r[4]}>" for r in rows if r[2] == "declined"]
        tentative = [f"<@{r[4]}>" for r in rows if r[2] == "tentative"]

        embed = discord.Embed(
            title=event[1],
            description=event[2],
            color=discord.Color.green(),
        )
        embed.add_field(name=f"🟩 Obecność ({len(accepted)})", value="\n".join(accepted) or "—", inline=True)
        embed.add_field(name=f"🟥 Nieobecność ({len(declined)})", value="\n".join(declined) or "—", inline=True)
        embed.add_field(name=f"🟦 Niepewność ({len(tentative)})", value="\n".join(tentative) or "—", inline=True)

        return embed

    # ---------------------- Update Embed After User Click ----------------------
    async def update_event_embed(discord_event_id, message):
        embed = build_embed(discord_event_id)
        await message.edit(embed=embed)

    # ---------------------- Scheduled Task ----------------------
    @tasks.loop(minutes=1)
    async def event_checker():
        garage_engine = create_engine(garage_connection_string)
        garage_connection = garage_engine.raw_connection()
        garage_cursor = garage_connection.cursor()

        # Find events that are 10 hours away and not posted yet
        garage_cursor.execute("""
            SELECT * FROM discord_bot_events dbe
            LEFT JOIN event e ON dbe.event_id = e.id
            WHERE dbe.posted = 0 AND e.start_date <= NOW() + INTERVAL 10 HOUR
        """)
        events = garage_cursor.fetchall()

        CHANNEL_ID = 857643204958879797

        channel = bot.get_channel(CHANNEL_ID)

        for event in events:
            embed = build_embed(event[0])
            view = AttendanceView(event[0])

            await channel.send(embed=embed, view=view)

            garage_cursor.execute("UPDATE discord_bot_events SET posted=1 WHERE id=%s", (event[0],))
            garage_connection.commit()

        garage_connection.close()

    bot.run(token)
