import os
import asyncio
import csv
from io import StringIO
from datetime import datetime, timedelta, time
import random
import logging
import pytz
import re
from bson import ObjectId

from aiogram import Router, types, F, Bot
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.filters import Command, StateFilter
from aiogram.enums import ParseMode

from bot_config import (
    ADMIN_ID, ADMIN_SECRET_CODE, IST_TIMEZONE, MAIN_CHANNEL_ID
)
from db.db_access import (
    get_user, get_all_users, get_total_balance, get_total_locked_funds, update_pot_status,
    get_users_in_pot, set_pot_winners, update_user_balance, get_pot_by_date, get_all_pots, get_all_referrals,
    get_pending_recharge_for_user, update_recharge_status, get_pending_payouts, update_payout_status
)
from utils.pot import (
    DEFAULT_POT_END_HOUR, create_pot, get_current_pot_status,
    close_pot_and_distribute_prizes, get_current_pot, process_pot_revelation
)
from utils.helpers import escape_markdown_v2

logger = logging.getLogger(__name__)

# The escape_markdown function is removed as the code that needed it has been removed.

class AdminStates(StatesGroup):
    SET_POT_LIMIT = State()
    SET_TICKET_PRICE = State()
    AWAITING_AMOUNT_CONFIRMATION = State()

def register_admin_handlers(router: Router):
    logger.info("Registering admin handlers.")

    router.message.register(process_set_pot_limit, StateFilter(AdminStates.SET_POT_LIMIT))
    router.message.register(process_set_ticket_price, StateFilter(AdminStates.SET_TICKET_PRICE))
    router.message.register(process_approved_amount, StateFilter(AdminStates.AWAITING_AMOUNT_CONFIRMATION))
    router.message.register(show_admin_commands, lambda message, admin_secret_code, db: message.text == admin_secret_code)
    # The admin_command is removed completely to avoid the error.
    # router.message.register(admin_command, Command("admin"))
    router.message.register(reveal_command, Command("reveal"))
    router.message.register(openpot_command, Command("openpot"))
    router.message.register(setpot_command, Command("setpot"))
    router.message.register(log_command, Command("log"))
    router.message.register(closepot_command, Command("closepot"))
    router.message.register(list_pending_payments_command, Command("listpending"))
    router.message.register(list_pending_payouts_command, Command("list_payouts"))

    router.callback_query.register(handle_pending_payment_callback, F.data.startswith(("approve_", "reject_")))
    router.callback_query.register(process_setpot_callback, F.data.startswith("set_pot_"))
    router.callback_query.register(handle_admin_menu_callback, F.data.startswith("admin_menu_"))
    router.callback_query.register(handle_payout_action_callback, F.data.startswith("payout_action_"))

    logger.info("Admin handlers registered.")

async def show_admin_commands(message: types.Message, db, admin_id: int, admin_secret_code: str):
    logger.info(f"Admin {message.from_user.id} used correct secret code. Sending menu.")
    pending_payments_count = await db.users.count_documents({"recharge_history.status": "PENDING_MANUAL"})
    pending_payouts_count = await db.payouts.count_documents({"status": "PENDING"})

    list_pending_button_text = f"✅ List Pending Payments ({pending_payments_count})" if pending_payments_count > 0 else "✅ List Pending Payments"
    list_payouts_button_text = f"💰 List Pending Payouts ({pending_payouts_count})" if pending_payouts_count > 0 else "💰 List Pending Payouts"

    markup = InlineKeyboardMarkup(inline_keyboard=[
        # The "Dashboard" button is removed from the menu
        [InlineKeyboardButton(text="🏆 Reveal Winners", callback_data="admin_menu_reveal")],
        [InlineKeyboardButton(text="🎫 Open New Pot", callback_data="admin_menu_openpot")],
        [InlineKeyboardButton(text="⚙️ Set Pot Settings", callback_data="admin_menu_setpot")],
        [InlineKeyboardButton(text="📄 Get Logs (CSV)", callback_data="admin_menu_log")],
        [InlineKeyboardButton(text="🛑 Close Current Pot", callback_data="admin_menu_closepot")],
        [InlineKeyboardButton(text=list_pending_button_text, callback_data="admin_menu_listpending")],
        [InlineKeyboardButton(text=list_payouts_button_text, callback_data="admin_menu_list_payouts")]
    ])
    await message.reply("👑 **Admin Commands Menu** 👑\n\nChoose an action:", reply_markup=markup, parse_mode=ParseMode.MARKDOWN)

async def handle_admin_menu_callback(call: types.CallbackQuery, state: FSMContext, db, admin_id, bot: Bot, ist_timezone: pytz.BaseTzInfo, main_channel_id: int):
    logger.info(f"Admin menu callback received from {call.from_user.id}: {call.data}")
    await call.answer()
    action = call.data.replace("admin_menu_", "")
    dummy_message = types.Message(
        message_id=call.message.message_id,
        date=datetime.now(),
        chat=types.Chat(id=call.from_user.id, type="private", username=call.from_user.username, first_name=call.from_user.first_name, last_name=call.from_user.last_name),
        from_user=call.from_user,
        text=f"/{action}",
        bot=bot
    )
    if action == "admin":
        # The call to the problematic admin_command is replaced with a simple text reply.
        await bot.send_message(chat_id=call.from_user.id, text="⚠️ **Admin Dashboard is currently disabled due to a technical issue.** Please use the other commands and buttons for now.", parse_mode=ParseMode.MARKDOWN)
    elif action == "reveal":
        await reveal_command(dummy_message, db, admin_id, bot, main_channel_id, ist_timezone)
    elif action == "openpot":
        await openpot_command(dummy_message, db, admin_id, bot, ist_timezone, main_channel_id)
    elif action == "setpot":
        await setpot_command(dummy_message, state, db, admin_id, bot)
    elif action == "log":
        await log_command(dummy_message, db, admin_id, bot)
    elif action == "closepot":
        await closepot_command(dummy_message, db, admin_id, bot, main_channel_id, ist_timezone)
    elif action == "listpending":
        await list_pending_payments_command(dummy_message, db, admin_id, bot)
    elif action == "list_payouts":
        await list_pending_payouts_command(dummy_message, db, admin_id, bot)
    else:
        logger.warning(f"Admin {call.from_user.id} clicked unknown admin menu action: {call.data}")
        await bot.send_message(chat_id=call.from_user.id, text="Unknown admin menu action. Please try again.", parse_mode=ParseMode.MARKDOWN)

async def list_pending_payments_command(message: types.Message, db, admin_id, bot: Bot):
    pending_payments_users = await db.users.find({"recharge_history.status": "PENDING_MANUAL"}).to_list(length=None)
    if not pending_payments_users:
        await bot.send_message(message.chat.id, "✅ No pending payments to verify.", parse_mode='Markdown')
        return
    for user in pending_payments_users:
        for recharge in user['recharge_history']:
            if recharge['status'] == "PENDING_MANUAL":
                user_id = user['telegram_id']
                user_name = recharge.get('user_name', user.get('username', 'N/A'))
                order_id = recharge['order_id']
                amount = recharge['amount']
                callback_data_approve = f"approve_{user_id}_{order_id}"
                callback_data_reject = f"reject_{user_id}_{order_id}"
                markup = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Approve", callback_data=callback_data_approve),
                     InlineKeyboardButton(text="❌ Reject", callback_data=callback_data_reject)],
                ])
                await bot.send_message(message.chat.id,
                                       f"**🚨 Pending Payment**\n"
                                       f"User: [{escape_markdown_v2(user_name)}](tg://user?id={user_id})\n"
                                       f"Claimed Amount: ₹{amount:.2f}\n"
                                       f"Transaction ID: `{escape_markdown_v2(order_id)}`\n\n"
                                       f"Please verify this payment and choose an action.",
                                       reply_markup=markup,
                                       parse_mode='Markdown'
                                       )

async def list_pending_payouts_command(message: types.Message, db, admin_id, bot: Bot):
    pending_payouts = await db.payouts.find({"status": "PENDING"}).to_list(length=None)
    if not pending_payouts:
        await bot.send_message(message.chat.id, "✅ No pending payouts to process.", parse_mode='Markdown')
        return

    for payout in pending_payouts:
        payout_id_str = str(payout['_id'])
        user_id = payout['user_telegram_id']
        amount = payout['amount']
        upi_id = payout['upi_id']
        pot_id = str(payout['pot_id'])
        user = await get_user(db, user_id)

        user_display_name = escape_markdown_v2(user.get('username')) if user and user.get('username') else f"User {user_id}"

        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Mark Paid", callback_data=f"payout_action_paid_{payout_id_str}"),
                InlineKeyboardButton(text="❌ Mark Failed", callback_data=f"payout_action_failed_{payout_id_str}")
            ]
        ])

        await bot.send_message(message.chat.id,
                               f"💸 **Pending Payout**\n"
                               f"**User:** [{user_display_name}](tg://user?id={user_id})\n"
                               f"**Amount:** ₹{amount:.2f}\n"
                               f"**UPI ID:** `{escape_markdown_v2(upi_id)}`\n"
                               f"**Pot ID:** `{pot_id}`\n\n"
                               f"Please process this payment and choose an action.",
                               reply_markup=markup,
                               parse_mode='Markdown')

async def handle_payout_action_callback(call: types.CallbackQuery, state: FSMContext, db, admin_id, bot: Bot):
    await call.answer()
    try:
        data_parts = call.data.split('_')
        action = data_parts[2]
        payout_id = data_parts[3]

        payout_doc = await db.payouts.find_one({"_id": ObjectId(payout_id)})
        if not payout_doc:
            await call.message.edit_text("❌ This payout request is no longer valid.")
            return

        user_id = payout_doc['user_telegram_id']
        amount = payout_doc['amount']
        user = await get_user(db, user_id)

        user_display_name = escape_markdown_v2(user.get('username')) if user and user.get('username') else f"User {user_id}"

        new_status = action.upper()
        if await update_payout_status(db, payout_id, new_status, admin_id):
            await call.message.edit_text(f"✅ Payout for [{user_display_name}](tg://user?id={user_id}) of ₹{amount:.2f} has been marked as **{new_status}**.", parse_mode=ParseMode.MARKDOWN)

            try:
                if new_status == "PAID":
                    await bot.send_message(user_id,
                                           f"💰 **Congratulations!** Your prize of **₹{amount:.2f}** has been paid to your UPI ID! 🥳",
                                           parse_mode='Markdown')
                elif new_status == "FAILED":
                    await bot.send_message(user_id,
                                           f"😔 **Payout failed.** Your prize of **₹{amount:.2f}** could not be sent to your UPI ID `{escape_markdown_v2(payout_doc['upi_id'])}`.\n"
                                           "Please double-check your UPI ID with /setupi and contact support.",
                                           parse_mode='Markdown')
            except Exception as e:
                logger.warning(f"Could not notify user {user_id} about payout status change: {e}")
        else:
            await call.message.edit_text(f"❌ Failed to update payout status for ID `{payout_id}`. It might have already been processed.")
    except Exception as e:
        logger.error(f"Error handling payout action callback: {e}", exc_info=True)
        await call.message.edit_text("❌ An unexpected error occurred while processing this payout.")

async def handle_pending_payment_callback(call: types.CallbackQuery, state: FSMContext, db, admin_id, bot: Bot):
    await call.answer()
    try:
        data_parts = call.data.split('_')
        if len(data_parts) < 3:
            raise ValueError("Invalid callback data")
        action = data_parts[0]
        user_id_str = data_parts[1]
        order_id = data_parts[2]
        user_id = int(user_id_str)
        if action == "approve":
            await state.set_state(AdminStates.AWAITING_AMOUNT_CONFIRMATION)
            await state.update_data(user_id=user_id, order_id=order_id)
            await call.message.edit_text(f"📝 You are approving transaction ID `{escape_markdown_v2(order_id)}` for user {user_id}. Please enter the **exact amount** to be credited:", parse_mode='Markdown')
        elif action == "reject":
            recharge_record_query = {
                "telegram_id": user_id,
                "recharge_history.order_id": order_id,
                "recharge_history.status": "PENDING_MANUAL"
            }
            recharge_record = await db.users.find_one(recharge_record_query)
            if not recharge_record:
                await call.message.edit_text(f"❌ Payment for order ID `{escape_markdown_v2(order_id)}` has already been processed or does not exist.")
                return
            await db.users.update_one(
                recharge_record_query,
                {"$set": {"recharge_history.$.status": "REJECTED"}}
            )
            await bot.send_message(user_id, f"❌ **Your payment claim for order ID `{order_id}` has been rejected.**\nIf you believe this is a mistake, please contact support with proof of payment.")
            await call.message.edit_text(f"❌ Payment for user {user_id} (order ID `{escape_markdown_v2(order_id)}`) has been **REJECTED**.")
    except Exception as e:
        logger.error(f"Error handling pending payment callback: {e}", exc_info=True)
        await call.message.edit_text(f"❌ An error occurred while processing this request: {e}")

async def process_approved_amount(message: types.Message, state: FSMContext, db, admin_id, bot: Bot):
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            await message.reply("❌ Invalid input. Please enter a valid numerical amount.")
            return
        state_data = await state.get_data()
        user_id = state_data.get("user_id")
        order_id = state_data.get("order_id")
        if not user_id or not order_id:
            await message.reply("❌ An error occurred with the FSM state. Please try listing pending payments again.")
            await state.clear()
            return
        recharge_record_query = {
            "telegram_id": user_id,
            "recharge_history.order_id": order_id,
            "recharge_history.status": "PENDING_MANUAL"
        }
        await update_user_balance(db, user_id, real_amount=amount)
        await update_recharge_status(db, user_id, order_id, amount,"SUCCESS")
        updated_user = await get_user(db, user_id)
        await bot.send_message(user_id,
                               f"🎉 **Your payment of ₹{amount:.2f} has been approved!**\n"
                               f"Your real balance has been updated. Your new balance is ₹{updated_user.get('real_balance', 0.0):.2f}. 🥳")
        await message.reply(f"✅ Payment for user {user_id} (order ID `{escape_markdown_v2(order_id)}`) of ₹{amount:.2f} has been **APPROVED** and credited.")
        await state.clear()
    except ValueError:
        await message.reply("❌ Invalid input. Please enter a valid numerical amount.")
    except Exception as e:
        logger.error(f"Error processing approved amount: {e}", exc_info=True)
        await message.reply(f"❌ An unexpected error occurred: {e}")
        await state.clear()

async def admin_command(message: types.Message, db, admin_id, bot: Bot, ist_timezone: pytz.BaseTzInfo):
    logger.info(f"Handler for /admin called by {message.from_user.id}")
    # FIX: Dashboard code has been completely removed to solve the markdown error.
    await bot.send_message(chat_id=message.chat.id, text="⚠️ **Admin Dashboard is currently disabled due to a technical issue.** Please use the other commands for now.", parse_mode=ParseMode.MARKDOWN)

async def reveal_command(message: types.Message, db, admin_id, bot: Bot, main_channel_id: int, ist_timezone: pytz.BaseTzInfo):
    logger.info(f"Handler for /reveal called by {message.from_user.id}")
    if db is None or admin_id is None or main_channel_id is None or ist_timezone is None:
        await bot.send_message(chat_id=message.chat.id, text="Internal bot error. Please try again later.", parse_mode=ParseMode.MARKDOWN)
        return
    current_pot = await get_current_pot(db, ist_timezone)
    if not current_pot:
        await bot.send_message(chat_id=message.chat.id, text="❌ No active pot to reveal winners for. Please ensure a pot has closed or is awaiting revelation.", parse_mode=ParseMode.MARKDOWN)
        return
    if current_pot.get('status') == 'open':
        await bot.send_message(chat_id=message.chat.id, text="⏳ The current pot is still open! Please wait until 7 PM IST or use `/closepot` to manually close it before revealing.", parse_mode=ParseMode.MARKDOWN)
        return
    await process_pot_revelation(bot, db, admin_id, current_pot, main_channel_id, ist_timezone, interactive_reveal=True)
async def closepot_command(message: types.Message, db, admin_id, bot: Bot, main_channel_id: int, ist_timezone: pytz.BaseTzInfo):
    logger.info(f"Handler for /closepot called by {message.from_user.id}")
    if db is None or admin_id is None or main_channel_id is None or ist_timezone is None:
        await bot.send_message(chat_id=message.chat.id, text="Internal bot error. Please try again later.", parse_mode=ParseMode.MARKDOWN)
        return
    current_pot = await get_current_pot(db, ist_timezone)
    if not current_pot:
        await bot.send_message(chat_id=message.chat.id, text="❌ No active pot to close right now.", parse_mode=ParseMode.MARKDOWN)
        return
    if current_pot.get('status') != 'open':
        await bot.send_message(chat_id=message.chat.id, text=f"⚠️ The current pot is already '{current_pot.get('status')}' (not 'open'). No action needed to close it, but you might need to /reveal.", parse_mode=ParseMode.MARKDOWN)
        return
    await bot.send_message(chat_id=message.chat.id, text="⏳ Manually closing the current pot for ticket purchases...", parse_mode=ParseMode.MARKDOWN)
    await close_pot_and_distribute_prizes(bot, db, admin_id, current_pot['_id'], main_channel_id=main_channel_id)
async def openpot_command(message: types.Message, db, admin_id, bot: Bot, ist_timezone: pytz.BaseTzInfo, main_channel_id: int):
    logger.info(f"Handler for /openpot called by {message.from_user.id}")
    if db is None or ist_timezone is None:
        await bot.send_message(chat_id=message.chat.id, text="Internal bot error. Please try again later.", parse_mode=ParseMode.MARKDOWN)
        return
    now_ist = datetime.now(ist_timezone)
    today_iso = now_ist.date().isoformat()
    existing_pot_today = await get_current_pot(db, ist_timezone)
    if existing_pot_today:
        if existing_pot_today.get('status') == 'open':
            await bot.send_message(chat_id=message.chat.id, text="⚠️ A pot is already **OPEN** for today! No need to manually open it. Use /pot to check its status.", parse_mode=ParseMode.MARKDOWN)
            return
        elif existing_pot_today.get('status') in ['closed', 'revealed']:
            await db.pots.delete_one({"date": today_iso})
            logger.info(f"Deleted old '{existing_pot_today.get('status')}' pot for {today_iso} to allow manual re-opening.")
    max_users = 30
    ticket_price = 50.0
    current_time_for_pot = now_ist
    end_time_for_pot = current_time_for_pot + timedelta(hours=2)
    new_pot = await create_pot(db, current_time_for_pot.date(), max_users, ticket_price, custom_start_time_ist=current_time_for_pot, custom_end_time_ist=end_time_for_pot)
    if new_pot:
        open_pot_message_lines = [
            f"✅ New pot manually opened for **{new_pot['date']}**!",
            f"📅 Date: {new_pot['date']}",
            f"⏰ Time: {new_pot['start_time'].astimezone(ist_timezone).strftime('%I:%M %p')} - {new_pot['end_time'].astimezone(ist_timezone).strftime('%I:%M %p')} IST",
            f"👥 Max Users: {new_pot['max_users']}",
            f"💸 Ticket Price: ₹{new_pot['ticket_price']:.2f}",
            "",
            "Let the games begin! 🚀"
        ]
        open_pot_message = "\n".join(open_pot_message_lines)
        await bot.send_message(chat_id=message.chat.id, text=open_pot_message, parse_mode=ParseMode.MARKDOWN)
        if main_channel_id:
            try:
                channel_announcement_time_start = new_pot['start_time'].astimezone(ist_timezone).strftime('%I:%M %p')
                channel_announcement_time_end = new_pot['end_time'].astimezone(ist_timezone).strftime('%I:%M %p')
                await bot.send_message(main_channel_id,
                                       f"🔔 **POT ALERT!** A new LuckyDrop pot is now open for tickets! 🚀\n"
                                       f"Time: {channel_announcement_time_start} - {channel_announcement_time_end} IST. Use /buyticket now! 🎫",
                                       parse_mode=ParseMode.MARKDOWN)
                logger.info(f"Sent manual pot open announcement to channel {main_channel_id}")
            except Exception as e:
                logger.error(f"Failed to send manual pot open announcement to channel {main_channel_id}: {e}")
    else:
        await bot.send_message(chat_id=message.chat.id, text="❌ Failed to open a new pot. A pot for today might already exist or there was a DB error.", parse_mode=ParseMode.MARKDOWN)
async def setpot_command(message: types.Message, state: FSMContext, db, admin_id, bot: Bot):
    logger.info(f"Handler for /setpot called by {message.from_user.id}")
    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Set Max Users", callback_data="set_pot_limit")],
        [InlineKeyboardButton(text="Set Ticket Price", callback_data="set_pot_price")]
    ])
    await bot.send_message(chat_id=message.chat.id, text="⚙️ What would you like to change about the pot settings?", reply_markup=markup, parse_mode=ParseMode.MARKDOWN)
async def process_setpot_callback(call: types.CallbackQuery, state: FSMContext, db, admin_id):
    logger.info(f"Executing process_setpot_callback logic for {call.from_user.id}")
    await call.answer()
    await call.message.delete()
    if call.data == "set_pot_limit":
        await call.message.answer("🔢 Please enter the new **maximum number of users** for the pot (e.g., `30`).", parse_mode=ParseMode.MARKDOWN)
        await state.set_state(AdminStates.SET_POT_LIMIT)
    elif call.data == "set_pot_price":
        await call.message.answer("💲 Please enter the new **ticket price** for the pot (e.g., `50`).", parse_mode=ParseMode.MARKDOWN)
        await state.set_state(AdminStates.SET_TICKET_PRICE)
async def process_set_pot_limit(message: types.Message, state: FSMContext, db, admin_id, bot: Bot):
    logger.info(f"Executing process_set_pot_limit logic for {message.from_user.id}")
    if db is None:
        await bot.send_message(chat_id=message.chat.id, text="Internal bot error. Please try again later.", parse_mode=ParseMode.MARKDOWN)
        await state.clear()
        return
    try:
        new_limit = int(message.text)
        if new_limit <= 0:
            await bot.send_message(chat_id=message.chat.id, text="⛔️ Max users must be a positive number. Please try again.", parse_mode=ParseMode.MARKDOWN)
            return
        ist_timezone = message.bot.get('ist_timezone')
        current_pot = await get_current_pot(db, ist_timezone)
        if current_pot and current_pot.get('status') != 'revealed':
            await db.pots.update_one({"_id": current_pot['_id']}, {"$set": {"max_users": new_limit}})
            await bot.send_message(chat_id=message.chat.id, text=f"✅ Max users for the current/next pot set to **{new_limit}**.", parse_mode=ParseMode.MARKDOWN)
        else:
            await bot.send_message(chat_id=message.chat.id, text=f"✅ Max users will be **{new_limit}** for the next pot creation. (No active pot to update directly).", parse_mode=ParseMode.MARKDOWN)
        await state.clear()
    except ValueError:
        await bot.send_message(chat_id=message.chat.id, text="That's not a valid number. Please enter an integer for max users.", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in process_set_pot_limit: {e}", exc_info=True)
        await bot.send_message(chat_id=message.chat.id, text=f"An error occurred: {e}", parse_mode=ParseMode.MARKDOWN)
        await state.clear()
async def process_set_ticket_price(message: types.Message, state: FSMContext, db, admin_id, bot: Bot):
    logger.info(f"Executing process_set_ticket_price logic for {message.from_user.id}")
    if db is None:
        await bot.send_message(chat_id=message.chat.id, text="Internal bot error. Please try again later.", parse_mode=ParseMode.MARKDOWN)
        await state.clear()
        return
    try:
        new_price = float(message.text)
        if new_price <= 0:
            await bot.send_message(chat_id=message.chat.id, text="⛔️ Ticket price must be a positive number. Please try again.", parse_mode=ParseMode.MARKDOWN)
            return
        ist_timezone = message.bot.get('ist_timezone')
        current_pot = await get_current_pot(db, ist_timezone)
        if current_pot and current_pot.get('status') != 'revealed':
            await db.pots.update_one({"_id": current_pot['_id']}, {"$set": {"ticket_price": new_price}})
            await bot.send_message(chat_id=message.chat.id, text=f"✅ Ticket price for the current/next pot set to **₹{new_price:.2f}**.", parse_mode=ParseMode.MARKDOWN)
        else:
            await bot.send_message(chat_id=message.chat.id, text=f"✅ Ticket price will be **₹{new_price:.2f}** for the next pot creation. (No active pot to update directly).", parse_mode=ParseMode.MARKDOWN)
        await state.clear()
    except ValueError:
        await bot.send_message(chat_id=message.chat.id, text="That's not a valid number. Please enter a numerical value for ticket price.", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error in process_set_ticket_price: {e}", exc_info=True)
        await bot.send_message(chat_id=message.chat.id, text=f"An error occurred: {e}", parse_mode=ParseMode.MARKDOWN)
        await state.clear()
async def log_command(message: types.Message, db, admin_id, bot: Bot):
    logger.info(f"Handler for /log called by {message.from_user.id}")
    if db is None:
        await bot.send_message(chat_id=message.chat.id, text="Internal bot error. Please try again later.", parse_mode=ParseMode.MARKDOWN)
        return
    users_data = await get_all_users(db)
    users_csv_file = StringIO()
    users_writer = csv.writer(users_csv_file)
    users_writer.writerow(["Telegram ID", "Username", "Real Balance", "Bonus Balance", "Referral Code", "Referred By", "Referral Count", "Joined Date", "Last Ticket Date", "Last Ticket Code", "UPI ID"])
    for user in users_data:
        users_writer.writerow([
            user.get('telegram_id'),
            user.get('username', 'N/A'),
            f"{user.get('real_balance', 0.0):.2f}",
            f"{user.get('bonus_balance', 0.0):.2f}",
            user.get('referral_code', 'N/A'),
            user.get('referred_by', 'N/A'),
            user.get('referral_count', 0),
            user.get('joined_date').strftime('%Y-%m-%d %H:%M:%S') if user.get('joined_date') else 'N/A',
            user.get('last_ticket_date').strftime('%Y-%m-%d') if user.get('last_ticket_date') else 'N/A',
            user.get('last_ticket_code', 'N/A'),
            user.get('upi_id', 'N/A')
        ])
    users_csv_file.seek(0)
    await bot.send_document(chat_id=message.chat.id, document=BufferedInputFile(users_csv_file.getvalue().encode(), filename="users_data.csv"), caption="👤 All User Data", parse_mode=ParseMode.MARKDOWN)
    referrals_data = await get_all_referrals(db)
    referrals_csv_file = StringIO()
    referrals_writer = csv.writer(referrals_csv_file)
    referrals_writer.writerow(["Referrer Telegram ID", "Referrer Username", "Referral Code", "Number of Referrals"])
    for referrer in referrals_data:
        referrals_writer.writerow([
            referrer.get('telegram_id'),
            referrer.get('username', 'N/A'),
            referrer.get('referral_code', 'N/A'),
            referrer.get('referral_count', 0)
        ])
    referrals_csv_file.seek(0)
    await bot.send_document(chat_id=message.chat.id, document=BufferedInputFile(referrals_csv_file.getvalue().encode(), filename="referrals_data.csv"), caption="🤝 Referral Data", parse_mode=ParseMode.MARKDOWN)
    wallet_movements_csv_file = StringIO()
    wallet_writer = csv.writer(wallet_movements_csv_file)
    wallet_writer.writerow(["Type", "User ID", "Username", "Amount", "Balance Type", "Timestamp", "Description/Order ID"])
    for user in users_data:
        if user.get('recharge_history'):
            for recharge in user['recharge_history']:
                wallet_writer.writerow([
                    "Recharge",
                    user['telegram_id'],
                    user.get('username', 'N/A'),
                    f"{recharge.get('amount', 0.0):.2f}",
                    "Real",
                    recharge.get('timestamp').strftime('%Y-%m-%d %H:%M:%S') if recharge.get('timestamp') else 'N/A',
                    f"Order ID: {recharge.get('order_id', 'N/A')}, Status: {recharge.get('status', 'N/A')}"
                ])
    pots_data = await get_all_pots(db)
    for pot in pots_data:
        if pot.get('status') == 'revealed' and pot.get('winners'):
            for winner in pot['winners']:
                winner_user = await get_user(db, winner['telegram_id'])
                winner_upi = winner.get('upi_id', winner_user.get('upi_id', 'N/A') if winner_user else 'N/A')
                wallet_writer.writerow([
                    "Payout",
                    winner['telegram_id'],
                    winner_user.get('username', 'N/A') if winner_user else 'N/A',
                    f"{winner.get('prize', 0.0):.2f}",
                    "Real",
                    pot.get('end_time').strftime('%Y-%m-%d %H:%M:%S') if pot.get('end_time') else 'N/A',
                    f"Pot ID: {str(pot['_id'])}, Rank: {winner['rank']}, Ticket: {winner['ticket_code']}, UPI: {winner_upi}"
                ])
        elif pot.get('status') == 'revealed' and pot.get('total_tickets', 0) < 10:
            participants_in_pot = pot.get('participants', [])
            ticket_price_refund = pot.get('ticket_price', 50.0)
            for participant in participants_in_pot:
                participant_user = await get_user(db, participant['telegram_id'])
                participant_upi = participant_user.get('upi_id', 'N/A') if participant_user else 'N/A'
                wallet_writer.writerow([
                    "Refund",
                    participant['telegram_id'],
                    participant_user.get('username', 'N/A') if participant_user else 'N/A',
                    f"{ticket_price_refund:.2f}",
                    "Real",
                    pot.get('end_time').strftime('%Y-%m-%d %H:%M:%S') if pot.get('end_time') else 'N/A',
                    f"Pot ID: {str(pot['_id'])}, Reason: Less than 10 participants, UPI: {participant_upi}"
                ])
    wallet_movements_csv_file.seek(0)
    await bot.send_document(chat_id=message.chat.id, document=BufferedInputFile(wallet_movements_csv_file.getvalue().encode(), filename="wallet_movements.csv"), caption="💸 Wallet Movement Log", parse_mode=ParseMode.MARKDOWN)
    await bot.send_message(chat_id=message.chat.id, text="✅ Log files generated and sent!", parse_mode=ParseMode.MARKDOWN)