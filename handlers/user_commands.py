import os
from datetime import datetime, time, timedelta
import random
import logging
import pytz
import re
from functools import wraps
from io import BytesIO

from aiogram import Router, types, F, Bot
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, Message, Chat, User
from aiogram.filters import Command
from aiogram.enums import ParseMode

logger = logging.getLogger(__name__)

from bot_config import (
    CASHFREE_RECHARGE_LINK, MIN_RECHARGE_AMOUNT, REFERRAL_BONUS,
    REVEAL_DELAY_MINUTES, ADMIN_ID, IST_TIMEZONE
)
from db.db_access import (
    get_user, create_user, update_user_balance, add_user_to_pot,
    update_user_ticket, check_referred_user_ticket_status,
    mark_referred_user_ticket_bought, increment_referral_count, update_user_upi,
    add_recharge_to_history, get_user_counts_by_referral_source, get_pending_payout_for_user,
    get_available_tickets, purchase_ticket_atomically, get_pending_recharge_for_user,
    get_referred_users_details
)
from utils.ticket import generate_unique_ticket_code, generate_ticket_image
from utils.pot import get_current_pot_status, get_current_pot
from utils.helpers import escape_markdown_v2

class ChannelJoinStates(StatesGroup):
    WAITING_FOR_CHANNEL_JOIN = State()

class UserStates(StatesGroup):
    WAITING_FOR_UPI_ID = State()
    CONFIRM_UPI_ID = State()
    WAITING_FOR_RECHARGE_DETAILS = State()
    CHOOSING_TICKET = State()
    CHOOSING_BONUS = State()


async def is_user_member_of_channel(bot: Bot, user_id: int, channel_id: int) -> bool:
    try:
        chat_member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return chat_member.status in ['creator', 'administrator', 'member', 'restricted']
    except Exception as e:
        logger.error(f"Error checking channel membership for user {user_id} in channel {channel_id}: {e}")
        return False


async def check_channel_membership(call: types.CallbackQuery, state: FSMContext, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Checking channel membership for user {call.from_user.id} via callback.")
    user_id = call.from_user.id

    if not main_channel_id:
        logger.error("MAIN_CHANNEL_ID not set in dispatcher context!")
        await call.message.edit_text("Bot configuration error: Main channel ID is missing. Please contact support.")
        await call.answer("Error!")
        await state.clear()
        return

    is_member = await is_user_member_of_channel(call.bot, user_id, main_channel_id)

    if is_member:
        user_data = await state.get_data()
        referrer_id = user_data.get('pending_referrer_id')

        user = await get_user(db, user_id)
        if not user:
            if referrer_id:
                referrer_user = await get_user(db, referrer_id)
                if referrer_user and referrer_user['telegram_id'] != user_id:
                    await create_user(db, user_id, call.from_user.username, referrer_id)
                    logger.info(f"New user {user_id} created after channel join with referrer: {referrer_id}")
                else:
                    await create_user(db, user_id, call.from_user.username, None)
                    logger.info(f"New user {user_id} created after channel join (invalid referrer).")
            else:
                await create_user(db, user_id, call.from_user.username, None)
                logger.info(f"New user {user_id} created after channel join (no referrer).")

        # FIX: Do not clear state here. The second /start command will handle it.
        # await state.clear() is removed.
        await call.message.edit_text("🎉 Great! You're now a member of our official channel! You can now use all bot features. Type /start again to see the welcome message.")
        await call.answer("Welcome aboard!")

    else:
        valid_channel_link = None
        try:
            channel_info = await call.bot.get_chat(main_channel_id)
            valid_channel_link = channel_info.invite_link or f"https://t.me/{channel_info.username}"
            if not channel_info.username and not channel_info.invite_link:
                valid_channel_link = None
                logger.warning(f"Could not get invite link or username for channel {main_channel_id}")
        except Exception as e:
            channel_link = "Error fetching channel link. Please contact support."
            logger.error(f"Failed to fetch channel info for {main_channel_id} during callback: {e}")
            valid_channel_link = None

        if valid_channel_link:
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Join Our Official Channel 🎉", url=valid_channel_link)],
                [InlineKeyboardButton(text="I have joined! ✅", callback_data="check_channel_membership")]
            ])
            await call.message.edit_text(
                "🛑 **Important!** To use LuckyDrop Bot, you must first join our official Telegram channel for important updates, results, and announcements!\n\n"
                "Please click the button below to join:",
                reply_markup=markup
            )
        else:
            await call.message.edit_text(
                "🛑 Not yet! Please join the channel first to unlock all features.\n"
                "Unfortunately, I couldn't get a direct link. Please search for the channel manually by its name/username and join. Then click 'I have joined!' again."
            )
        await state.set_state(ChannelJoinStates.WAITING_FOR_CHANNEL_JOIN)
        return

    await call.answer()


async def start_command(message: types.Message, state: FSMContext, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /start called by {message.from_user.id}. Message text: {message.text}")
    # FIX: Move state clearing to the end of a successful start

    if db is None or main_channel_id is None:
        logger.error("DB or MAIN_CHANNEL_ID instance not available in start_command!")
        await message.reply("Internal bot error: Configuration missing. Please contact support.")
        await state.clear()
        return

    user_id = message.from_user.id
    is_member = await is_user_member_of_channel(message.bot, user_id, main_channel_id)

    if not is_member:
        logger.info(f"User {user_id} is not a member of the main channel. Prompting to join.")

        referrer_id = None
        args = message.text.split(maxsplit=1)
        if len(args) > 1 and args[1].startswith("LUCKY"):
            referrer_code = args[1]
            referrer_user = await db.users.find_one({"referral_code": referrer_code})
            if referrer_user and referrer_user['telegram_id'] != user_id:
                referrer_id = referrer_user['telegram_id']
                await state.update_data(pending_referrer_id=referrer_id)
                logger.info(f"Referral code {referrer_code} saved to state for user {user_id}.")

        valid_channel_link = None
        try:
            channel_info = await message.bot.get_chat(main_channel_id)
            valid_channel_link = channel_info.invite_link or f"https://t.me/{channel_info.username}"
            if not channel_info.username and not channel_info.invite_link:
                valid_channel_link = None
                logger.warning(f"Could not get invite link or username for channel {main_channel_id}")
        except Exception as e:
            channel_link = "Error fetching channel link. Please contact support."
            logger.error(f"Failed to fetch channel info for {main_channel_id} during callback: {e}")
            valid_channel_link = None

        if valid_channel_link:
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Join Our Official Channel 🎉", url=valid_channel_link)],
                [InlineKeyboardButton(text="I have joined! ✅", callback_data="check_channel_membership")]
            ])
            await message.reply(
                "🛑 **Important!** To use LuckyDrop Bot, you must first join our official Telegram channel for important updates, results, and announcements!\n\n"
                "Please click the button below to join:",
                reply_markup=markup
            )
        else:
            await message.reply(
                "🛑 Not yet! Please join the channel first to unlock all features.\n"
                "Unfortunately, I couldn't get a direct link. Please search for the channel manually by its name/username and join. Then click 'I have joined!' again."
            )
        await state.set_state(ChannelJoinStates.WAITING_FOR_CHANNEL_JOIN)
        return

    user = await get_user(db, user_id)

    welcome_message = (
        "👋 Welcome to **LuckyDrop Bot**, where daily drops can make your day! 🤑\n\n"
        "Here's how to play:\n"
        "✨ We have one daily pot from **5:00 PM to 7:00 PM IST**.\n"
        "🎟️ Buy a ticket for ₹50. **Max one ticket per user per pot!**\n"
        "🍀 Each ticket has a **unique 6-digit code**.\n"
        "💰 Tickets are bought using up to ₹30 bonus + ₹20 real balance.\n"
        "🏅 After **7:00 PM IST**, winners are chosen randomly from all **sold tickets** and the full prize pool is awarded if we hit 30 users:\n"
        "   🥇 1st: ₹500\n"
        "   🥈 2nd: ₹200\n"
        "   🥉 3rd: ₹100\n"
        "📉 If 10-29 users, prizes scale proportionally. If <10, all refunds!\n"
        "💸 **Winners get paid to their UPI ID within 12 hours of results!**\n\n"
        "Use these commands:\n"
        "/wallet — Check your balance and recharge\n"
        "/buyticket — Grab your lucky ticket\n"
        "/refer — Share the luck & earn bonuses\n"
        "/pot — See the current pot's status\n"
        "/setupi — Register or update your UPI ID\n"
        "/help — Get a quick reminder on how to play\n\n"
        "Good luck, future winner! 🚀"
    )

    if not user:
        await create_user(db, user_id, message.from_user.username, None)
        user = await get_user(db, user_id)
        logger.info(f"New user {user_id} created from regular /start command (already a member).")

    await message.reply(welcome_message)
    # FIX: Clear state only after the entire successful start flow is complete
    await state.clear()


def check_channel_membership_decorator(func):
    @wraps(func)
    async def wrapper(message: types.Message, *args, **kwargs):
        db = kwargs.get('db')
        main_channel_id = kwargs.get('main_channel_id')
        state = kwargs.get('state')
        ist_timezone = kwargs.get('ist_timezone')

        if db is None or main_channel_id is None:
            logger.error(f"Config error in {func.__name__} decorator: db or main_channel_id missing.")
            await message.reply("Bot configuration error. Please contact support.")
            return

        user_id = message.from_user.id
        is_member = await is_user_member_of_channel(message.bot, user_id, main_channel_id)

        if not is_member:
            logger.info(f"User {user_id} tried {message.text} but is not a channel member. Prompting to join.")

            referrer_id = None
            args = message.text.split(maxsplit=1)
            if len(args) > 1 and args[1].startswith("LUCKY"):
                referrer_code = args[1]
                referrer_user = await db.users.find_one({"referral_code": referrer_code})
                if referrer_user and referrer_user['telegram_id'] != user_id:
                    referrer_id = referrer_user['telegram_id']
                    await state.update_data(pending_referrer_id=referrer_id)
                    logger.info(f"Referral code {referrer_code} saved to state for user {user_id}.")

            valid_channel_link = None
            try:
                channel_info = await message.bot.get_chat(main_channel_id)
                valid_channel_link = channel_info.invite_link or f"https://t.me/{channel_info.username}"
            except Exception:
                valid_channel_link = "Error fetching channel link. Please contact support."

            if valid_channel_link and not valid_channel_link.startswith("Error fetching"):
                markup = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="Join Our Official Channel 🎉", url=valid_channel_link)],
                    [InlineKeyboardButton(text="I have joined! ✅", callback_data="check_channel_membership")]
                ])
                await message.reply(
                    "🛑 **Important!** To use LuckyDrop Bot, you must first join our official Telegram channel for important updates, results, and announcements!\n\n"
                    "Please click the button below to join:",
                    reply_markup=markup
                )
            else:
                await message.reply(
                    "🛑 Not yet! Please join the channel first to unlock all features.\n"
                    "Unfortunately, I couldn't get a direct link. Please search for the channel manually by its name/username and join. Then click 'I have joined!' again."
                )
            await state.set_state(ChannelJoinStates.WAITING_FOR_CHANNEL_JOIN)
            return

        kwargs['ist_timezone'] = ist_timezone
        return await func(message, *args, **kwargs)
    return wrapper


async def wallet_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /wallet called by {message.from_user.id}")
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.reply("Looks like you're new here! Please use /start to register. 🤖")
        return

    pending_recharge = await get_pending_recharge_for_user(db, message.from_user.id)

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="I have paid! ✅", callback_data="recharge_paid")]
    ])

    if pending_recharge:
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Check Recharge Status ⏳", callback_data="recharge_status_check")]
        ])

    await message.reply(
        f"💰 Your Wallet:\n"
        f"💵 Real Balance: ₹{user.get('real_balance', 0.0):.2f}\n"
        f"🎁 Bonus Balance: ₹{user.get('bonus_balance', 0.0):.2f}\n\n"
        f"To recharge, click here: [Cashfree Link]({CASHFREE_RECHARGE_LINK})\n"
        f"**_Important: When paying, please enter your name and email on the Cashfree site._**\n\n"
        f"After paying, click the button below and provide your payment details. We will update your wallet once approved. ⏳",
        reply_markup=markup,
        disable_web_page_preview=True
    )


async def recharge_status_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /recharge_status called by {message.from_user.id}")
    pending_recharge = await get_pending_recharge_for_user(db, message.from_user.id)

    response_message = "✅ You have no pending recharge requests. Use /wallet to recharge your balance."
    if pending_recharge:
        response_message = (
            f"⏳ **Your pending recharge request:**\n"
            f"Amount: ₹{pending_recharge.get('amount'):.2f}\n"
            f"Transaction ID: `{pending_recharge.get('order_id')}`\n"
            f"Status: **Pending Admin Approval**\n\n"
            f"An admin will verify it shortly. You'll be notified once it's confirmed!"
        )

    await message.reply(response_message, parse_mode='Markdown')

async def recharge_status_callback(call: types.CallbackQuery, db, admin_id, main_channel_id, ist_timezone):
    await call.answer()
    logger.info(f"Handler for recharge_status_check callback called by {call.from_user.id}")

    pending_recharge = await get_pending_recharge_for_user(db, call.from_user.id)

    response_message = "✅ You have no pending recharge requests. Use /wallet to recharge your balance."
    if pending_recharge:
        response_message = (
            f"⏳ **Your pending recharge request:**\n"
            f"Amount: ₹{pending_recharge.get('amount'):.2f}\n"
            f"Transaction ID: `{pending_recharge.get('order_id')}`\n"
            f"Status: **Pending Admin Approval**\n\n"
            f"An admin will verify it shortly. You'll be notified once it's confirmed!"
        )

    await call.message.edit_text(response_message, parse_mode='Markdown')


async def prompt_for_recharge_details(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(UserStates.WAITING_FOR_RECHARGE_DETAILS)
    await call.message.edit_text("👍 Great! Please reply to this message with your payment details in the following format:\n\n"
                                 "`[Your Name]`\n`[Amount Paid]`\n`[Transaction ID]`\n\n"
                                 "Example:\n`Aniket`\n`50`\n`CF123456789`", parse_mode='Markdown')

async def process_recharge_details(message: types.Message, state: FSMContext, db, admin_id):
    user_id = message.from_user.id
    try:
        parts = message.text.strip().split('\n')
        if len(parts) != 3:
            raise ValueError("Incorrect format")

        user_name = parts[0].strip()
        amount_str = parts[1].strip()
        transaction_id = parts[2].strip()

        amount = float(amount_str)
        if amount <= 0:
            raise ValueError("Amount must be positive")

        await add_recharge_to_history(db, user_id, amount=amount, status="PENDING_MANUAL", order_id=transaction_id, user_name=user_name)

        markup = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_{user_id}_{transaction_id}"),
                InlineKeyboardButton(text="❌ Reject", callback_data=f"reject_{user_id}_{transaction_id}")
            ]
        ])

        await message.bot.send_message(admin_id,
            f"**🚨 New Pending Payment!**\n"
            f"From User: [{escape_markdown_v2(user_name)}](tg://user?id={user_id})\n"
            f"Claimed Amount: ₹{amount:.2f}\n"
            f"Transaction ID: `{escape_markdown_v2(transaction_id)}`",
            reply_markup=markup,
            parse_mode='Markdown'
        )

        await message.reply("⏳ We have received your payment claim! An admin will verify it shortly. You will be notified once your wallet is updated. Thanks for your patience! 🙏")
        await state.clear()
    except ValueError:
        await message.reply("❌ Invalid format. Please make sure you enter your details on three separate lines as instructed.")
    except Exception as e:
        logger.error(f"Error processing recharge details from user {user_id}: {e}", exc_info=True)
        await message.reply("An unexpected error occurred. Please try again.")
        await state.clear()

async def buyticket_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone, state: FSMContext):
    logger.info(f"Handler for /buyticket called by {message.from_user.id}")
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.reply("Please use /start to register before buying a ticket. 🚀")
        return

    current_pot = await get_current_pot(db, ist_timezone)
    if not current_pot or current_pot.get('status') != 'open':
        await message.reply("🎟️ The pot is currently closed! Ticket sales are open daily from **5:00 PM to 7:00 PM IST.** Stay tuned! ⏳")
        return

    user_tickets_in_current_pot = [p['telegram_id'] for p in current_pot.get('participants', [])]
    if user['telegram_id'] in user_tickets_in_current_pot:
        await message.reply(f"🚫 You've already bought your ticket for this pot! Your ticket code is: `{user['last_ticket_code']}`. Good luck! 🤞")
        return

    all_tickets = await db.tickets.find({"pot_id": current_pot['_id']}).to_list(length=None)
    sold_ticket_codes = {p['ticket_code'] for p in current_pot.get('participants', [])}

    sold_count = len(sold_ticket_codes)
    max_users = current_pot.get('max_users', 30)

    keyboard = []
    row = []

    for ticket in all_tickets:
        if ticket['code'] in sold_ticket_codes:
            row.append(InlineKeyboardButton(text=f"🔒 {ticket['code']}", callback_data=f"ticket_sold_{ticket['code']}"))
        else:
            row.append(InlineKeyboardButton(text=f"🍀 {ticket['code']}", callback_data=f"buy_ticket_{ticket['code']}"))

        if len(row) == 3:
            keyboard.append(row)
            row = []

    if row:
        keyboard.append(row)

    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    message_text = (
        "🐺🔥 **The pot is LIVE!** 🔥🐺\n"
        f"**Tickets Sold: {sold_count}/{max_users}**\n\n"
        "Your destiny is just a click away. Select your lucky number and secure your spot in today's prize draw.\n\n"
        "**Legend:**\n"
        "🍀 - Available Tickets (Your opportunity)\n"
        "🔒 - Sold Tickets (Missed chance)\n\n"
        "This is not a game of chance, it's a game of speed. The pot is filling fast! Don't get left behind! 🚀"
    )

    await message.reply(
        message_text,
        reply_markup=reply_markup
    )

    await state.set_state(UserStates.CHOOSING_TICKET)


async def process_ticket_selection(call: types.CallbackQuery, state: FSMContext, db, admin_id, main_channel_id, ist_timezone):
    await call.answer()

    ticket_code = call.data.split('_')[2]
    user_id = call.from_user.id

    user = await get_user(db, user_id)
    current_pot = await get_current_pot(db, ist_timezone)
    ticket_price = current_pot.get('ticket_price', 50.0)

    if user.get('real_balance', 0.0) + user.get('bonus_balance', 0.0) < ticket_price:
        await call.message.edit_text(
            f"💸 Oh no! Your total balance (`Real: ₹{user.get('real_balance', 0.0):.2f}, Bonus: ₹{user.get('bonus_balance', 0.0):.2f}`) "
            f"is not enough to buy ticket `{ticket_code}` for `₹{ticket_price:.2f}`.\n\n"
            f"Please recharge your wallet with /wallet first, then try again."
        )
        await state.clear()
        return

    await state.update_data(chosen_ticket=ticket_code)

    bonus_balance = user.get('bonus_balance', 0.0)
    keyboard = []

    if bonus_balance >= 30:
        keyboard.append([InlineKeyboardButton(text="Use ₹30 from Bonus", callback_data=f"select_bonus_30")])
    if bonus_balance >= 20:
        keyboard.append([InlineKeyboardButton(text="Use ₹20 from Bonus", callback_data=f"select_bonus_20")])
    if bonus_balance >= 10:
        keyboard.append([InlineKeyboardButton(text="Use ₹10 from Bonus", callback_data=f"select_bonus_10")])

    if user.get('real_balance', 0.0) >= ticket_price:
        keyboard.append([InlineKeyboardButton(text="Use only Real Cash", callback_data=f"select_bonus_0")])

    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    await call.message.edit_text(
        "💰 **How would you like to pay for ticket `{}`?**\n\n"
        "Your balance: `Real: ₹{:.2f}, Bonus: ₹{:.2f}`\n"
        "Ticket Price: `₹{:.2f}`\n\n"
        "Please select a payment method below.".format(
            ticket_code, user.get('real_balance', 0.0), user.get('bonus_balance', 0.0), ticket_price
        ),
        reply_markup=reply_markup
    )

    await state.set_state(UserStates.CHOOSING_BONUS)


async def process_bonus_selection_and_buy(call: types.CallbackQuery, state: FSMContext, db, admin_id, main_channel_id, ist_timezone):
    await call.answer("Finalizing your purchase...")

    state_data = await state.get_data()
    ticket_code = state_data.get('chosen_ticket')

    if not ticket_code:
        await call.message.edit_text("❌ An error occurred. Please try the /buyticket command again.")
        await state.clear()
        return

    selected_bonus_amount = int(call.data.split('_')[2])
    user_id = call.from_user.id

    user = await get_user(db, user_id)
    current_pot = await get_current_pot(db, ist_timezone)
    ticket_price = current_pot.get('ticket_price', 50.0)

    bonus_to_use = min(selected_bonus_amount, user.get('bonus_balance', 0.0))
    real_needed = ticket_price - bonus_to_use

    if user.get('real_balance', 0.0) < real_needed:
        await call.message.edit_text(f"💸 You don't have enough real balance to cover this. You need ₹{real_needed:.2f} more. Please recharge your wallet with /wallet.")
        await state.clear()
        return

    purchase_success = await purchase_ticket_atomically(db, current_pot['_id'], user_id, ticket_code)

    if not purchase_success:
        await call.message.edit_text(f"Oh no! Ticket `{ticket_code}` was just sold. Please choose another ticket from the list below.")
        await state.clear()
        await buyticket_command(call.message, db, admin_id, main_channel_id, ist_timezone, state)
        return

    await update_user_balance(db, user_id, real_amount=-real_needed, bonus_amount=-bonus_to_use)
    await update_user_ticket(db, user_id, ticket_code)

    referrer_id = user.get('referred_by')
    if referrer_id:
        referrer_user = await get_user(db, referrer_id)

        if referrer_user and user_id not in referrer_user.get('referred_users_tickets', []):

            logger.info(f"User {user_id} is buying their first ticket. Crediting referrer {referrer_id} with bonus...")

            await db.users.update_one(
                {"telegram_id": referrer_id},
                {"$inc": {"bonus_balance": REFERRAL_BONUS, "referral_count": 1},
                 "$push": {"referred_users_tickets": user_id}}
            )

            try:
                updated_referrer = await get_user(db, referrer_id)
                referrer_balance = updated_referrer.get('bonus_balance', 0.0)
                await call.bot.send_message(
                    referrer_id,
                    f"🎉 **Referral Bonus Alert!** 🎉\n"
                    f"Your friend has bought their first ticket! You have been credited with a **₹{REFERRAL_BONUS:.2f} bonus!**\n"
                    f"Your new bonus balance is ₹{referrer_balance:.2f}. Keep referring to earn more! 🤝"
                )
                logger.info(f"Bonus of {REFERRAL_BONUS} credited to referrer {referrer_id}.")
            except Exception as e:
                logger.error(f"Failed to notify referrer {referrer_id} about bonus: {e}")
        else:
            logger.info(f"User {user_id} has already been credited for a previous ticket purchase. No bonus awarded.")

    try:
        user_id_str = str(user.get('telegram_id'))
        image_path = generate_ticket_image(code=ticket_code, user_id=user_id_str, referral_name=None)

        if image_path and os.path.exists(image_path):
            await call.message.answer_photo(photo=FSInputFile(image_path))
            os.remove(image_path)

        await call.message.answer(
            f"🎉 **CONGRATULATIONS!** 🎉\n"
            f"Your lucky ticket `{ticket_code}` is now in the draw!\n"
            f"You've taken the first step towards a win. Good luck! 🍀",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Error during image generation for user {user_id}: {e}", exc_info=True)
        await call.message.edit_text(
            f"🎉 Success! You've got your lucky ticket for today! 🎉\n"
            f"Your ticket code: `{ticket_code}`\n"
            f"Used: ₹{bonus_to_use:.2f} (Bonus) + ₹{real_needed:.2f} (Real)\n"
            f"Good luck! The draw happens automatically shortly after the pot closes. 🍀",
            parse_mode='Markdown'
        )


    filled_count = len(current_pot.get('participants', [])) + 1
    max_users = current_pot.get('max_users', 30)
    if filled_count == max_users:
        await call.bot.send_message(admin_id, f"🔔 **ATTENTION ADMIN!** The pot is now FULL! ({filled_count}/{max_users} users).")

    await state.clear()


async def handle_sold_ticket_click(call: types.CallbackQuery):
    await call.answer("This ticket is already sold. Please select an available one.", show_alert=True)


async def refer_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /refer called by {message.from_user.id}")
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.reply("Please use /start to register first. 🤖")
        return

    referral_code = user.get('referral_code', f"LUCKY{user['telegram_id']}")
    bot_info = await message.bot.get_me()
    referral_link = f"https://t.me/{bot_info.username}?start={referral_code}"
    referral_count = user.get('referral_count', 0)

    referred_users = await get_referred_users_details(db, user['telegram_id'])
    referred_users_message = ""
    if referred_users:
        referred_users_message = "\n\n**Your Referrals:**\n"
        for i, ref_user in enumerate(referred_users):
            username_display = escape_markdown_v2(ref_user.get('username')) if ref_user.get('username') else f"User {ref_user['telegram_id']}"
            status = "🎟️ Bought Ticket" if ref_user.get('bought_ticket') else "⏳ Just Joined"
            referred_users_message += f"{i+1}. [{username_display}](tg://user?id={ref_user['telegram_id']}) - {status}\n"
    else:
        referred_users_message = "\n\n_You haven't referred any users yet._"

    final_message_text = (
        f"🤝 Share the luck! Refer your friends and earn a **₹{REFERRAL_BONUS:.2f} bonus** for each friend "
        f"who joins and buys their first ticket!\n\n"
        f"Your unique referral code: `{escape_markdown_v2(referral_code)}`\n"
        f"Your referral link: [{escape_markdown_v2('Click here to invite!')}]({referral_link})\n\n"
        f"You have successfully referred **{referral_count}** friends! 🚀"
        f"{referred_users_message}"
    )

    await message.reply(
        final_message_text,
        disable_web_page_preview=False
    )


async def pot_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /pot called by {message.from_user.id}")
    current_pot = await get_current_pot(db, ist_timezone)

    if not current_pot:
        await message.reply("⏳ There's no active pot right now. Ticket sales open daily from **5:00 PM to 7:00 PM IST!** Get ready! 🔔")
        return

    pot_status_message = await get_current_pot_status(pot_data=current_pot, db=db, ist_timezone=ist_timezone)
    await message.reply(pot_status_message)


async def help_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /help called by {message.from_user.id}")
    help_text = (
        "🤔 **How to Play LuckyDrop Bot:**\n\n"
        "1.  **Daily Pot:** A new lottery pot opens every day from **5:00 PM to 7:00 PM IST**. 🕰️\n"
        "2.  **Buy a Ticket:** Use `/buyticket` to purchase your lucky entry. Each ticket costs **₹50**. "
        "You can use up to **₹30 from your bonus balance** and the rest from your real balance. **Only one ticket per user per pot!**\n"
        "3.  **Unique Code:** Every ticket has a **unique 6-digit code**.\n"
        "4.  **Wallet:** Check your `real_balance` and `bonus_balance` with `/wallet`. Recharge your real balance manually by paying via the Cashfree link and submitting your payment details for admin approval.\n"
        "5.  **Refer & Earn:** Share your unique referral link (get it with `/refer`). When a friend joins via your link, "
        "starts the bot, and buys their first ticket, you get a **₹10 bonus!** 🤝\n"
        "6.  **Pot Status:** See how many tickets are sold with `/pot`.\n"
        "7.  **UPI Payouts:** Winners get paid directly to their UPI ID. Use `/setupi` to register or update your UPI ID. **Payouts are processed within 12 hours of results!**\n\n"
        "🏆 **Winning Rules (Draw after 7:00 PM IST):**\n"
        "-   **Less than 10 users:** Everyone gets a full refund to their **real** wallet. No hard feelings! ↩️\n"
        "-   **10 to 29 users:** Prizes scale proportionally. If you want to know more about the scaled prizes read the previous logs\n"
        "-   **30 users (Full Pot):**\n"
        "   🥇 1st Prize: ₹500\n"
        "   🥈 2nd: ₹200\n"
        "   🥉 3rd: ₹100\n\n"
        "Winners are chosen **fairly and randomly from all SOLD tickets** by the system and announced automatically shortly after the pot closes. Keep an eye out! 👀\n\n"
        "Got it? Let's get lucky! ✨"
    )
    await message.reply(help_text)


async def setupi_command(message: types.Message, state: FSMContext, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /setupi called by {message.from_user.id}")
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.reply("Please use /start to register first. 🤖")
        return

    current_upi = user.get('upi_id')
    if current_upi:
        await message.reply(f"Your current UPI ID is: `{escape_markdown_v2(current_upi)}`\n"
                             "Do you want to update it? Please send your new UPI ID now.", parse_mode='Markdown')
    else:
        await message.reply("Please send your UPI ID (e.g., `yourname@bank` or `phonenumber@upi`).\n"
                             "This is where your winnings will be sent! 💰", parse_mode='Markdown')

    await state.set_state(UserStates.WAITING_FOR_UPI_ID)


async def process_upi_input(message: types.Message, state: FSMContext, db, admin_id, main_channel_id, ist_timezone):
    user_id = message.from_user.id
    upi_id_raw = message.text.strip()

    if not re.match(r"^[\w.\-]{2,}@[a-zA-Z]{2,}$", upi_id_raw):
        await message.reply("That doesn't look like a valid UPI ID format. Please try again.\n"
                             "Example: `yourname@bank` or `phonenumber@upi`\n"
                             "Make sure it contains an `@` symbol and a dot (`.`) in the domain part.", parse_mode='Markdown')
        return

    await state.update_data(new_upi_id=upi_id_raw)

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Yes, confirm", callback_data=f"confirm_upi_yes")],
        [InlineKeyboardButton(text="❌ No, re-enter", callback_data=f"confirm_upi_no")]
    ])

    await message.reply(f"You entered: `{escape_markdown_v2(upi_id_raw)}`\n"
                         "Is this correct? Once confirmed, this will be used for your payouts.", reply_markup=markup, parse_mode='Markdown')
    await state.set_state(UserStates.CONFIRM_UPI_ID)


async def confirm_upi_callback(call: types.CallbackQuery, state: FSMContext, db, admin_id, main_channel_id, ist_timezone):
    await call.answer()
    user_id = call.from_user.id
    user_data = await state.get_data()
    new_upi_id = user_data.get('new_upi_id')

    if call.data == "confirm_upi_yes" and new_upi_id:
        current_user = await get_user(db, user_id)
        current_upi = current_user.get('upi_id')

        await update_user_upi(db, user_id, new_upi_id)

        pending_payout = await get_pending_payout_for_user(db, user_id, window_hours=10)

        if pending_payout:
            username = current_user.get('username')
            if username:
                user_name_display = escape_markdown_v2(username)
            else:
                user_name_display = f"User {user_id}"

            try:
                admin_message = (
                    f"🔔 **URGENT: WINNER'S UPI ID UPDATED!** 🔔\n\n"
                    f"**User:** [{user_name_display}](tg://user?id={user_id})\n"
                    f"**New UPI ID:** `{escape_markdown_v2(new_upi_id)}`\n"
                    f"**Prize Amount:** ₹{pending_payout['amount']:.2f}\n"
                    f"Please process this payout manually. The user's UPI ID has been set/updated within the 10-hour window."
                )
                await call.bot.send_message(admin_id, admin_message, parse_mode='Markdown')
            except Exception as e:
                logger.error(f"Failed to notify admin about UPI update for winner {user_id}: {e}")

        await call.message.edit_text(f"🎉 Your UPI ID has been set to: `{escape_markdown_v2(new_upi_id)}`\n"
                                     "You're all set for payouts! 💰", parse_mode='Markdown')
        logger.info(f"User {user_id} confirmed and set UPI ID: {new_upi_id}")
    else:
        await call.message.edit_text("UPI ID not confirmed. Please send your UPI ID again if you wish to set it.")
        await state.set_state(UserStates.WAITING_FOR_UPI_ID)

    await state.clear()


# Moved registration function to the end of the file
def register_user_handlers(router: Router):
    logger.info("Registering user handlers...")

    router.message.register(start_command, Command("start"))
    router.message.register(wallet_command, Command("wallet"))
    router.message.register(buyticket_command, Command("buyticket"))
    router.message.register(refer_command, Command("refer"))
    router.message.register(pot_command, Command("pot"))
    router.message.register(help_command, Command("help"))
    router.message.register(setupi_command, Command("setupi"))
    router.message.register(recharge_status_command, Command("recharge_status"))

    # New handler for the inline button callback
    router.callback_query.register(recharge_status_callback, F.data == "recharge_status_check")

    router.message.register(process_upi_input, UserStates.WAITING_FOR_UPI_ID)
    router.callback_query.register(confirm_upi_callback, F.data.startswith("confirm_upi_"), UserStates.CONFIRM_UPI_ID)

    router.callback_query.register(prompt_for_recharge_details, F.data == "recharge_paid")
    router.message.register(process_recharge_details, UserStates.WAITING_FOR_RECHARGE_DETAILS)

    # The channel check callback will now also handle user creation
    router.callback_query.register(check_channel_membership, F.data == "check_channel_membership", ChannelJoinStates.WAITING_FOR_CHANNEL_JOIN)

    router.callback_query.register(process_bonus_selection_and_buy, UserStates.CHOOSING_BONUS, F.data.startswith("select_bonus_"))
    router.callback_query.register(process_ticket_selection, UserStates.CHOOSING_TICKET, F.data.startswith("buy_ticket_"))
    router.callback_query.register(handle_sold_ticket_click, F.data.startswith("ticket_sold_"))
    router.callback_query.register(handle_sold_ticket_click, UserStates.CHOOSING_TICKET, F.data.startswith("ticket_sold_"))