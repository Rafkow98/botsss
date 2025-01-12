import random

import pandas as pd

import discord
from discord import MessageType
from discord.ext import commands
from sqlalchemy import create_engine, text


def run_bot(connection_string, token):
    intents = discord.Intents.all()
    intents.members = True

    bot = commands.Bot(command_prefix="!", intents=intents)
    bot.remove_command("help")

    engine = create_engine(connection_string)
    connection = engine.raw_connection()
    cursor = connection.cursor()

    bots = [968851237405597717, 475744554910351370, 235148962103951360, 762217899355013120, 159985870458322944,
            1311665961027244084, 734535151899639910, 1311665961027244084]

    gifs = [
        'https://media.discordapp.net/attachments/902300785466040370/980959555666182194/kowalski.gif?ex=6779b4c7&is=67786347&hm=bbc24b6f3bf4f57baff91e56d2fb47f1d40c2537a9af1ff610fd3b0de3350e9f&',
        'https://media1.tenor.com/m/c9WptHOa_LMAAAAd/pong.gif',
        'https://media1.tenor.com/m/nRhIzRSb9lcAAAAd/hamuj-jabłonowski.gif',
        'https://cdn.discordapp.com/attachments/815522218213244958/1327760435856015511/ezgif-5-3b1bf3bb1d.gif?ex=67843cc4&is=6782eb44&hm=48e92cc855316c44ebfd867fcf28bb66f85b778f0560d131f50321e1722dccf9&',
        'https://media.discordapp.net/attachments/834503336979857471/1231702797871742976/ezgif.com-animated-gif-maker.gif?ex=677ce55e&is=677b93de&hm=2b95d9277e621cd4ed0340818a887faf69fe6c0f6a5ac97d2aed8c48b2f34566&=']

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

    @bot.event
    async def on_ready():
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

    @bot.event
    async def on_member_join(member):
        with engine.begin() as cnx:
            cnx.execute(text("INSERT INTO members(id, name) VALUES(:id, :name)"),
                        {'id': member.id, 'name': member.name})

        """channel = (bot.get_channel(1311665281462042624) or await bot.fetch_channel(1311665281462042624))
        await channel.send(content=f'Welcome to the server, {member.mention}! <:brzoza:857643791165685821>',
                           file=discord.File("img/witamywsss.gif", filename="welcome.gif"))"""

    @bot.event
    async def on_member_remove(member):
        with engine.begin() as cnx:
            cnx.execute(text("DELETE FROM members WHERE id = :id"), {'id': member.id})

        """channel = await bot.fetch_channel(1311665281462042624)
        await channel.send(f'{member.mention} has left the server. Goodbye! <:brzoza:857643791165685821>',
                           file=discord.File("img/discordsss.jpg", filename="welcome.gif"))"""

    @bot.event
    async def on_message(message):
        if bot.user.mentioned_in(message) and message.type != MessageType.reply:
            await message.reply(random.choice(gifs))
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
        if ctx.channel.id != 985565935849070702 and ctx.channel.id != 799608563566772234:
            return
        try:
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
                            f"Poziom toksyczności użytkownika {user.mention} wynosi {round(toxic_count / total_count * 100, 3)}%.")
                    else:
                        await ctx.reply(f"Użytkownik {user.mention} ma mniej niż 10 toksycznych wiadomości na serwerze")
                else:
                    await ctx.reply(f"Użytkownik {user.mention} ma mniej niż 100 wiadomości na serwerze")
        except discord.ext.commands.errors.UserNotFound:
            await ctx.reply(f"Użytkownika nie ma na serwerze")

    @bot.command(name='racism')
    async def count_racist_messages(ctx, user: discord.User = False):
        if ctx.channel.id != 985565935849070702 and ctx.channel.id != 799608563566772234:
            return
        try:
            if not user:
                user = ctx.author
            if user.id not in bots:
                cursor.execute("SELECT all_24 FROM messages_count WHERE author_id = " + str(user.id))
                total_count = cursor.fetchone()[0]
                cursor.execute("SELECT racism_24 FROM messages_count WHERE author_id = " + str(user.id))
                racist_count = cursor.fetchone()[0]
                connection.commit()

                if total_count > 100:
                    if racist_count > 10:
                        await ctx.reply(
                            f"Poziom rasizmu użytkownika {user.mention} wynosi {round(racist_count / total_count * 100, 3)}%.")
                    else:
                        await ctx.reply(
                            f"Użytkownik {user.mention} ma mniej niż 10 rasistowskich wiadomości na serwerze")
                else:
                    await ctx.reply(f"Użytkownik {user.mention} ma mniej niż 100 wiadomości na serwerze")
        except discord.ext.commands.errors.UserNotFound:
            await ctx.reply(f"Użytkownika nie ma na serwerze")

    @bot.command(name='avatar')
    async def get_avatar(ctx, user: discord.User = False):
        if not user:
            user = ctx.author
        av = user.avatar.url
        await ctx.reply(content=f'Avatar użytkownika {user.mention}', embed=discord.Embed().set_image(url=av))

    @bot.command(name='help')
    async def get_help(ctx):
        await ctx.reply('## Dostępne komendy:\n\n'
                        '### Kanał https://discord.com/channels/733758693971066960/985565935849070702:\n'
                        '**!avatar [@user]** - wyświetla avatar użytkownika\n'
                        '**!best [@user]** - wyświetla wiadomość z największą liczbą pojedynczej reakcji\n'
                        '**!bestall [@user]** - wyświetla wiadomość z największą liczbą wszystkich reakcji\n'
                        '**!first [@user]** - wyświetla pierwszą wiadomość użytkownika\n'
                        '**!messages** - ranking liczby wiadomości na serwerze (TOP10)\n'
                        '**!reactions** - ranking liczby dodanych reakcji (TOP10)\n'
                        '**!reacted** - ranking liczby otrzymanych reakcji (TOP10)\n'
                        '**!stats [@user]** - statystyki użytkownika - liczba wiadomości oraz dodanych i otrzymanych reakcji\n'
                        '**!racism [@user]** - wyświetla poziom rasizmu użytkownika (procent wiadomości, '
                        'użytkownik musi mieć minimum 100 wiadomości i 10 rasistowskich wiadomości na serwerze)\n'
                        '**!toxic [@user]** - wyświetla poziom toksyczności użytkownika (procent wiadomości, '
                        'użytkownik musi mieć minimum 100 wiadomości i 10 toksycznych wiadomości na serwerze)\n\n'
                        '### Wszystkie kanały:\n'
                        '**!czystobylo** - losowy gif z serii czysto było\n'
                        '**!help** - zbiór dostępnych komend\n'
                        '**!facebook | fb** - link do Facebooka\n'
                        '**!twitch | tt** - link do Twitcha\n'
                        '**!youtube | yt** - link do YouTube\n\n'
                        '**[]** - opcjonalny argument\n'
                        '**|** - alias')

    @bot.command(name='facebook', aliases=['fb'])
    async def get_fb(ctx):
        await ctx.reply('https://www.facebook.com/SimSprintSeries')

    @bot.command(name='twitch', aliases=['tt'])
    async def get_tt(ctx):
        await ctx.reply('https://www.twitch.tv/SimSprintSeries')

    @bot.command(name='youtube', aliases=['yt'])
    async def get_yt(ctx):
        await ctx.reply('https://www.youtube.com/SimSprintSeries')

    @bot.command(name='first')
    async def get_first_message(ctx, user: discord.User = False):
        if ctx.channel.id != 985565935849070702:
            return
        if not user:
            user = ctx.author
        cursor.execute(
            "SELECT id FROM messages WHERE author_id = " + str(user.id) + " AND type NOT LIKE 'GuildMemberJoin' "
                                                                          "ORDER BY DATE(timestamp) LIMIT 1")
        try:
            message_id = cursor.fetchone()[0]
            cursor.execute("SELECT channel_id FROM messages WHERE id = " + str(message_id))
            channel_id = cursor.fetchone()[0]
            await ctx.reply(f"Pierwsza wiadomość użytkownika {user.mention}: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik {user.mention} nie napisał żadnej wiadomości na tym serwerze")
        connection.commit()

    @bot.command(name='best')
    async def get_best_message(ctx, user: discord.User = False):
        if ctx.channel.id != 985565935849070702 and ctx.channel.id != 799608563566772234:
            return
        if not user:
            user = ctx.author
        cursor.execute("SELECT message_id FROM reactions WHERE author_id = " + str(user.id) +
                       " GROUP BY message_id, reaction_id ORDER BY COUNT(*) DESC LIMIT 1")
        try:
            message_id = cursor.fetchone()[0]
            cursor.execute("SELECT channel_id FROM messages WHERE id = " + str(message_id))
            channel_id = cursor.fetchone()[0]
            await ctx.reply(f"Wiadomość użytkownika {user.mention} z największą liczbą jednej reakcji: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik {user.mention} nie napisał żadnej wiadomości na tym serwerze")
        connection.commit()

    @bot.command(name='bestall')
    async def get_best_message_all(ctx, user: discord.User = False):
        if ctx.channel.id != 985565935849070702 and ctx.channel.id != 799608563566772234:
            return
        if not user:
            user = ctx.author
        cursor.execute("SELECT message_id FROM reactions WHERE author_id = " + str(user.id) +
                       " GROUP BY message_id ORDER BY COUNT(*) DESC LIMIT 1")
        try:
            message_id = cursor.fetchone()[0]
            cursor.execute("SELECT channel_id FROM messages WHERE id = " + str(message_id))
            channel_id = cursor.fetchone()[0]
            await ctx.reply(f"Wiadomość użytkownika {user.mention} z największą liczbą wszystkich reakcji: "
                            f"https://discord.com/channels/{ctx.guild.id}/{channel_id}/{message_id}")
        except TypeError:
            await ctx.reply(f"Użytkownik {user.mention} nie napisał żadnej wiadomości na tym serwerze")
        connection.commit()

    @bot.command(name='czystobylo')
    async def czystobylo(message):
        await message.reply(random.choice(czysto))

    @bot.command(name='stats')
    async def get_stats(ctx, user: discord.User = False):
        if ctx.channel.id != 985565935849070702 and ctx.channel.id != 799608563566772234 and ctx.channel.id != 857643204958879797:
            return
        if not user:
            user = ctx.author
        cursor.execute("SELECT `all`, reaction, reacted FROM messages_count WHERE author_id = " + str(user.id))
        result = cursor.fetchone()
        all_messages = result[0]
        reactions = result[1]
        reacted = result[2]
        connection.commit()
        try:
            await ctx.reply(f"Statystyki użytkownika {user.mention}:\n"
                            f"Liczba wiadomości: {all_messages}\n"
                            f"Liczba dodanych reakcji: {reactions}\n"
                            f"Liczba otrzymanych reakcji: {reacted}")
        except discord.ext.commands.errors.UserNotFound:
            await ctx.reply(f"Użytkownika nie ma na serwerze")

    @bot.command(name='messages')
    async def get_stats(ctx):
        if ctx.channel.id != 985565935849070702 and ctx.channel.id != 799608563566772234 and ctx.channel.id != 857643204958879797:
            return
        cursor.execute("SELECT author_id, `all` FROM messages_count ORDER BY `all` DESC LIMIT 10")
        result = cursor.fetchall()
        connection.commit()
        final = ''
        for i in range(len(result)):
            try:
                user = bot.get_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
            except AttributeError:
                user = await bot.fetch_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
        await ctx.reply('Najwięcej wiadomości na serwerze (tylko otwarte i istniejące kanały):\n' + final)

    @bot.command(name='reactions')
    async def get_reactions(ctx):
        if ctx.channel.id != 985565935849070702 and ctx.channel.id != 799608563566772234 and ctx.channel.id != 857643204958879797:
            return
        cursor.execute("SELECT author_id, reaction FROM messages_count ORDER BY reaction DESC LIMIT 10")
        result = cursor.fetchall()
        connection.commit()
        final = ''
        for i in range(len(result)):
            try:
                user = bot.get_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
            except AttributeError:
                user = await bot.fetch_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
        await ctx.reply('Najwięcej dodanych reakcji na serwerze (tylko otwarte i istniejące kanały):\n' + final)

    @bot.command(name='reacted')
    async def get_reacted(ctx):
        if ctx.channel.id != 985565935849070702 and ctx.channel.id != 799608563566772234 and ctx.channel.id != 857643204958879797:
            return
        cursor.execute("SELECT author_id, reacted FROM messages_count ORDER BY reacted DESC LIMIT 10")
        result = cursor.fetchall()
        connection.commit()
        final = ''
        for i in range(len(result)):
            try:
                user = bot.get_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
            except AttributeError:
                user = await bot.fetch_user(result[i][0])
                final += str(i) + '. ' + user.name + ': ' + str(result[i][1]) + '\n'
            except discord.errors.NotFound:
                final += str(i) + '. usunięty użytkownik: ' + str(result[i][1]) + '\n'
        await ctx.reply('Najwięcej otrzymanych reakcji na serwerze (tylko otwarte i istniejące kanały):\n' + final)

    bot.run(token)
