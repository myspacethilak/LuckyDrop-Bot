import os
import logging
from datetime import datetime, time, timedelta
import asyncio
import pytz
import sys
import traceback

from aiogram import Bot, Dispatcher, types, Router, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ParseMode
from aiogram.types import BotCommand
from aiogram.client.default import DefaultBotProperties

from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web

# Import config settings
from bot_config import (
    BOT_TOKEN, ADMIN_ID, MONGO_URI, MAIN_CHANNEL_ID, ADMIN_SECRET_CODE,
    IST_TIMEZONE, UTC_TIMEZONE
)

# Import other modules from new locations
from db.db_access import init_db
from handlers.user_commands import register_user_handlers, UserStates
from handlers.admin_commands import register_admin_handlers, AdminStates
from utils.pot import close_pot_and_distribute_prizes, schedule_daily_pot_open, get_current_pot
from utils.payment import cashfree_webhook_handler

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


# Initialize bot with default properties
default_properties = DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
bot = Bot(token=BOT_TOKEN, default=default_properties)

storage = MemoryStorage()
dp = Dispatcher(storage=storage)

user_router = Router(name="user_router")
admin_router = Router(name="admin_router")

db_client = AsyncIOMotorClient(MONGO_URI)
db = db_client.lotterydb

pot_scheduler_task = None

def log_unhandled_exceptions(exctype, value, tb):
    with open('error.log', 'a') as f:
        f.write(f"[{datetime.now()}] Unhandled Exception:\n")
        traceback.print_exception(exctype, value, tb, file=f)
    print("An unhandled exception occurred and was saved to error.log.")
    sys.exit(1)

async def set_default_commands(bot: Bot):
    commands = [
        BotCommand(command="start", description="🚀 Get started with LuckyDrop!"),
        BotCommand(command="wallet", description="💰 Check your wallet balance"),
        BotCommand(command="buyticket", description="🎟️ Buy your daily lucky ticket"),
        BotCommand(command="refer", description="🤝 Get your referral link and earn!"),
        BotCommand(command="pot", description="📊 See current pot status"),
        BotCommand(command="help", description="❓ Learn how to play"),
        BotCommand(command="setupi", description="Set or update your UPI ID"),
    ]
    await bot.set_my_commands(commands)

async def close_overdue_pots_on_startup(db, bot, ist_timezone: pytz.BaseTzInfo, utc_timezone: pytz.BaseTzInfo):
    current_time_ist = datetime.now(ist_timezone)
    pot_data = await get_current_pot(db, ist_timezone)

    if pot_data and pot_data.get('status') == 'open':
        pot_end_time_from_db = pot_data['end_time']
        if pot_end_time_from_db.tzinfo is None:
            pot_end_time_from_db = utc_timezone.localize(pot_end_time_from_db)

        pot_end_time_ist = pot_end_time_from_db.astimezone(ist_timezone)

        if pot_end_time_ist < current_time_ist:
            logger.info(f"Found overdue pot {pot_data['_id']} (ends {pot_end_time_ist.strftime('%I:%M %p IST')}), closing it now.")
            admin_id = ADMIN_ID
            main_channel_id = MAIN_CHANNEL_ID
            await close_pot_and_distribute_prizes(bot, db, admin_id, pot_data['_id'], main_channel_id=main_channel_id)


async def on_startup(dispatcher: Dispatcher, bot: Bot):
    global pot_scheduler_task

    dispatcher['db'] = db
    dispatcher['admin_id'] = ADMIN_ID
    dispatcher['main_channel_id'] = MAIN_CHANNEL_ID
    dispatcher['admin_secret_code'] = ADMIN_SECRET_CODE
    dispatcher['ist_timezone'] = IST_TIMEZONE
    dispatcher['utc_timezone'] = UTC_TIMEZONE

    logger.info(f"Dispatcher context set: db={db is not None}, admin_id={ADMIN_ID}, main_channel_id={MAIN_CHANNEL_ID}, admin_secret_code={'***' if ADMIN_SECRET_CODE else 'None'}, timezone='Asia/Kolkata'")

    await init_db(db)
    logger.info("Bot started and database initialized!")

    register_user_handlers(user_router)
    register_admin_handlers(admin_router)

    dp.include_router(user_router)
    admin_router.message.filter(lambda message, admin_id: message.from_user.id == admin_id)
    admin_router.callback_query.filter(lambda call, admin_id: call.from_user.id == admin_id)
    dp.include_router(admin_router)
    logger.info("Routers included successfully.")

    await set_default_commands(bot)
    logger.info("Default commands set.")

    pot_scheduler_task = asyncio.create_task(schedule_daily_pot_open(bot, db, ADMIN_ID, MAIN_CHANNEL_ID, IST_TIMEZONE, UTC_TIMEZONE))
    logger.info("Pot scheduler task started.")

    await close_overdue_pots_on_startup(db, bot, IST_TIMEZONE, UTC_TIMEZONE)


async def on_shutdown(dispatcher: Dispatcher, bot: Bot):
    logger.info("Shutting down bot and cleaning up tasks...")
    if pot_scheduler_task and not pot_scheduler_task.done():
        pot_scheduler_task.cancel()
        try:
            await pot_scheduler_task
            logger.info("Pot scheduler task was cancelled.")
        except asyncio.CancelledError:
            logger.info("Pot scheduler task was cancelled.")

    db_client.close()
    logger.info("MongoDB connection closed.")
    logger.info("Bot shutdown complete.")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', lambda r: web.Response(text="Bot is running!"))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', os.getenv('PORT', 8080))
    await site.start()

async def main():
    server_task = asyncio.create_task(start_web_server())
    polling_task = asyncio.create_task(dp.start_polling(bot, skip_updates=True))
    await asyncio.gather(server_task, polling_task)


if __name__ == '__main__':
    sys.excepthook = log_unhandled_exceptions

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    asyncio.run(main())