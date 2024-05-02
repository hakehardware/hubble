import aiohttp
import asyncio
import discord

from discord import Webhook
from src.logger import logger

class DiscordAPI:

    @staticmethod
    async def send_message(url, message, title, alert_type) -> None:

        if alert_type == 'error':
            color = discord.Color.red()
        else:
            color = discord.Color.green()

        async with aiohttp.ClientSession() as session:
            webhook = Webhook.from_url(url, session=session)
            embed = discord.Embed(
                title=title,
                description=message,
                color=color
            )
            await webhook.send(embed=embed)

    @staticmethod
    def send_discord_message(url, message, title, alert_type):
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(
                DiscordAPI.send_message(url, message, title, alert_type)
            )
            loop.close()
        except Exception as e:
            logger.error("Error in task:", exc_info=e)