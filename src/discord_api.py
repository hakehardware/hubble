import aiohttp
import asyncio
import discord

from discord import Webhook

class DiscordAPI:

    @staticmethod
    async def send_message(url, message, title):
        async with aiohttp.ClientSession() as session:
            webhook = Webhook.from_url(url, session=session)
            embed = discord.Embed(
                title=title,
                description=message,
                color=discord.Color.green()
            )
            await webhook.send(embed=embed)

    def send_discord_message(url, message, title):

        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            DiscordAPI.send_message(url, message, title)
        )
        loop.close()