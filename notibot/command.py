import logging

from discord.ext import commands

bot = commands.Bot(command_prefix="!")


@bot.event
async def on_ready():
    logging.info("Bot is ready.")


@bot.command(pass_context=True)
async def set_channel(ctx: commands.context.Context, channel: str):
    logging.info(f"set channel to {channel}")
    await ctx.send(f"Set channel to {channel}..")
    # do something
    await ctx.send(f"Channel is set to {channel}.")


bot.run("INSERT TOKEN HERE")
