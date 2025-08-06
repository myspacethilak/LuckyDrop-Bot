import os
from datetime import datetime, time, timedelta
import random
import logging
import pytz
import re
from functools import wraps
from io import BytesIO

# NEW: The Pillow imports are now only in ticket.py, not here.
# from PIL import Image, ImageDraw, ImageFont

from aiogram import Router, types, F, Bot
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
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
    add_recharge_to_history, get_user_counts_by_referral_source
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

def register_user_handlers(router: Router):
    logger.info("Registering user handlers...")

    router.message.register(start_command, Command("start"))
    router.message.register(wallet_command, Command("wallet"))
    router.message.register(buyticket_command, Command("buyticket"))
    router.message.register(refer_command, Command("refer"))
    router.message.register(pot_command, Command("pot"))
    router.message.register(help_command, Command("help"))
    router.message.register(setupi_command, Command("setupi"))

    router.message.register(process_upi_input, UserStates.WAITING_FOR_UPI_ID)
    router.callback_query.register(confirm_upi_callback, F.data.startswith("confirm_upi_"), UserStates.CONFIRM_UPI_ID)

    router.callback_query.register(prompt_for_recharge_details, F.data == "recharge_paid")
    router.message.register(process_recharge_details, UserStates.WAITING_FOR_RECHARGE_DETAILS)

    router.callback_query.register(check_channel_membership, F.data == "check_channel_membership", ChannelJoinStates.WAITING_FOR_CHANNEL_JOIN)
    logger.info("User handlers registered.")

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
        await call.message.edit_text("🎉 Great! You're now a member of our official channel! You can now use all bot features. Type /start again to see the welcome message.")
        await call.answer("Welcome aboard!")
        await state.clear()
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
    logger.info(f"Handler for /start called by {message.from_user.id}")
    await state.clear()

    if db is None or main_channel_id is None:
        logger.error("DB or MAIN_CHANNEL_ID instance not available in start_command!")
        await message.reply("Internal bot error: Configuration missing. Please contact support.")
        return

    user_id = message.from_user.id
    is_member = await is_user_member_of_channel(message.bot, user_id, main_channel_id)

    if not is_member:
        logger.info(f"User {user_id} is not a member of the main channel. Prompting to join.")
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
        "🎟️ Buy a ticket for ₹50. Max one ticket per user per day!\n"
        "🍀 Each ticket has a **unique 6-digit code**.\n"
        "💰 Tickets are bought using up to ₹30 bonus + ₹20 real balance.\n"
        "🏅 After **7:00 PM IST**, winners are chosen randomly by our fair system and the full prize pool is awarded if we hit 30 users:\n"
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
        referrer_id = None
        args = message.text.split(maxsplit=1)
        if len(args) > 1 and args[1].startswith("LUCKY"):
            referrer_code = args[1]
            referrer_user = await db.users.find_one({"referral_code": referrer_code})
            if referrer_user and referrer_user['telegram_id'] != user_id:
                referrer_id = referrer_user['telegram_id']
                referrer_username_display = escape_markdown_v2(referrer_user['username']) if referrer_user.get('username') else str(referrer_user['telegram_id'])
                welcome_message += f"\n\n_You were referred by user [{referrer_username_display}](tg://user?id={referrer_user['telegram_id']})!_"
            else:
                welcome_message += "\n\n_Invalid referral code._"

        user = await create_user(db, user_id, message.from_user.username, referrer_id)

    await message.reply(welcome_message)


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


@check_channel_membership_decorator
async def wallet_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /wallet called by {message.from_user.id}")
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.reply("Looks like you're new here! Please use /start to register. 🤖")
        return

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="I have paid! ✅", callback_data="recharge_paid")]
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

async def buyticket_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /buyticket called by {message.from_user.id}")
    user = await get_user(db, message.from_user.id)
    if not user:
        await message.reply("Please use /start to register before buying a ticket. 🚀")
        return

    current_pot = await get_current_pot(db, ist_timezone)
    if not current_pot or current_pot.get('status') != 'open':
        await message.reply("🎟️ The pot is currently closed! Ticket sales are open daily from **5:00 PM to 7:00 PM IST.** Stay tuned! ⏳")
        return

    today_date = datetime.now(ist_timezone).date()
    user_last_ticket_date_ist = None
    if user.get('last_ticket_date'):
        if user['last_ticket_date'].tzinfo is None:
            user_last_ticket_date_ist = pytz.utc.localize(user['last_ticket_date']).astimezone(ist_timezone).date()
        else:
            user_last_ticket_date_ist = user['last_ticket_date'].astimezone(ist_timezone).date()

    if user_last_ticket_date_ist == today_date and \
       any(p.get('telegram_id') == user['telegram_id'] for p in current_pot.get('participants', [])):
        await message.reply(f"🚫 You've already bought your ticket for today's pot! Your ticket code is: `{user['last_ticket_code']}`. Good luck! 🤞")
        return

    ticket_price = current_pot.get('ticket_price', 50.0)
    bonus_to_use = min(user.get('bonus_balance', 0.0), 30.0)
    real_needed = ticket_price - bonus_to_use

    if user.get('real_balance', 0.0) < real_needed:
        remaining_needed = real_needed - user.get('real_balance', 0.0)
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Recharge Now 💳", url=CASHFREE_RECHARGE_LINK)],
            [InlineKeyboardButton(text="I have paid! ✅", callback_data="recharge_paid")]
        ])
        await message.reply(
            f"💸 Oh no! You need **₹{real_needed:.2f}** from your real balance to buy this ticket "
            f"(after using ₹{bonus_to_use:.2f} from bonus). "
            f"You only have ₹{user.get('real_balance', 0.0):.2f} real balance. "
            f"Please recharge at least ₹{max(MIN_RECHARGE_AMOUNT, remaining_needed):.2f}).",
            reply_markup=markup
        )
        return

    user_before_deduction = await get_user(db, message.from_user.id)
    if not user_before_deduction:
        await message.reply("Error: Could not verify your balance. Please try again.")
        logger.error(f"User {message.from_user.id} not found right before ticket deduction.")
        return

    bonus_to_deduct = min(user_before_deduction.get('bonus_balance', 0.0), 30.0)
    real_to_deduct = ticket_price - bonus_to_use

    if user_before_deduction.get('real_balance', 0.0) < real_to_deduct:
        await message.reply("Error: Insufficient real balance after re-check. Please try again.")
        logger.error(f"User {message.from_user.id} insufficient real balance on re-check.")
        return

    await update_user_balance(db, message.from_user.id, real_amount=-real_to_deduct, bonus_amount=-bonus_to_deduct)

    updated_user_after_deduction = await get_user(db, message.from_user.id)

    ticket_code = await generate_unique_ticket_code(db)

    await add_user_to_pot(db, current_pot['_id'], message.from_user.id, ticket_code)
    await update_user_ticket(db, message.from_user.id, ticket_code)

    if user.get('referred_by'):
        referrer_id = user['referred_by']
        already_credited = await check_referred_user_ticket_status(db, referrer_id, user['telegram_id'])
        if not already_credited:
            referrer_user = await get_user(db, referrer_id)
            if referrer_user:
                await update_user_balance(db, referrer_id, bonus_amount=REFERRAL_BONUS)
                await increment_referral_count(db, referrer_id)
                await mark_referred_user_ticket_bought(db, referrer_id, user['telegram_id'])
                try:
                    referred_username_display = escape_markdown_v2(user.get('username')) if user.get('username') else str(user['telegram_id'])
                    await message.bot.send_message(referrer_id,
                        f"🎉 **Referral Bonus!** Your referred friend "
                        f"[{referred_username_display}](tg://user?id={user['telegram_id']}) "
                        f"just bought their first ticket! You've received a ₹{REFERRAL_BONUS:.2f} bonus! 🥳"
                    )
                except Exception as e:
                    logger.warning(f"Could not send referral bonus message to {referrer_id}: {e}")

    pot_end_time_utc = current_pot.get('end_time')
    if pot_end_time_utc and ist_timezone:
        pot_end_time_ist = pot_end_time_utc.astimezone(ist_timezone)
        announcement_time_ist = pot_end_time_ist + timedelta(minutes=REVEAL_DELAY_MINUTES)
        end_time_display = pot_end_time_ist.strftime('%I:%M %p IST')
        announcement_time_display = announcement_time_ist.strftime('%I:%M %p IST')
    else:
        end_time_display = 'pot closing time'
        announcement_time_display = 'soon after'

    try:
        user_id_str = str(user.get('telegram_id'))

        image_path = generate_ticket_image(code=ticket_code, user_id=user_id_str, referral_name=None)

        if image_path and os.path.exists(image_path):
            await message.answer_photo(photo=FSInputFile(image_path))

            await message.reply(
                f"🥳 **Your LuckyDrop ticket has been secured!**\n\n"
                f"Your ticket code `{ticket_code}` is now in the draw for today's pot.\n"
                f"The pot closes at **{end_time_display}**, with winners announced at **{announcement_time_display}**.\n\n"
                f"Good luck! ✨ May fortune smile upon you in today's draw."
            )
            os.remove(image_path)
        else:
            await message.reply(f"Ticket booked! But failed to generate image. Your code is: `{ticket_code}`")

    except Exception as e:
        logger.error(f"Error during image generation for user {message.from_user.id}: {e}", exc_info=True)
        await message.reply(
            f"🎉 Success! You've got your lucky ticket for today! 🎉\n"
            f"Your ticket code: `{ticket_code}`\n"
            f"Used: ₹{bonus_to_duct:.2f} (Bonus) + ₹{real_to_deduct:.2f} (Real)\n"
            f"New balances: Real: ₹{updated_user_after_deduction.get('real_balance', 0.0):.2f}, Bonus: ₹{updated_user_after_deduction.get('bonus_balance', 0.0):.2f}\n"
            f"Good luck! The draw happens automatically shortly after the pot closes. 🍀"
        )

    current_pot = await get_current_pot(db, ist_timezone)
    if current_pot:
        filled_count = len(current_pot['participants'])
        max_users = current_pot['max_users']
        if filled_count == max_users:
            await message.bot.send_message(admin_id, f"🔔 **ATTENTION ADMIN!** The pot is now FULL! ({filled_count}/{max_users} users).")
        elif filled_count >= max_users * 0.9 and filled_count < max_users:
            await message.bot.send_message(admin_id, f"📢 **Heads Up Admin!** The pot is almost full! ({filled_count}/{max_users} users).")


@check_channel_membership_decorator
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

    escaped_referral_code_display = escape_markdown_v2(referral_code)
    clickable_referral_link_text = escape_markdown_v2("Click here to invite!")
    clickable_referral_link = f"[{clickable_referral_link_text}]({referral_link})"

    final_message_text = (
        f"🤝 Share the luck! Refer your friends and earn a **₹{REFERRAL_BONUS:.2f} bonus** for each friend "
        f"who joins and buys their first ticket!\n\n"
        f"Your unique referral code: `{escaped_referral_code_display}`\n"
        f"Your referral link: {clickable_referral_link}\n\n"
        f"You have successfully referred **{referral_count}** friends! 🚀"
    )

    await message.reply(
        final_message_text,
        disable_web_page_preview=False
    )

@check_channel_membership_decorator
async def pot_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /pot called by {message.from_user.id}")
    current_pot = await get_current_pot(db, ist_timezone)

    if not current_pot:
        await message.reply("⏳ There's no active pot right now. Ticket sales open daily from **5:00 PM to 7:00 PM IST!** Get ready! 🔔")
        return

    pot_status_message = await get_current_pot_status(pot_data=current_pot, db=db, ist_timezone=ist_timezone)
    await message.reply(pot_status_message)

@check_channel_membership_decorator
async def help_command(message: types.Message, db, admin_id, main_channel_id, ist_timezone):
    logger.info(f"Handler for /help called by {message.from_user.id}")
    help_text = (
        "🤔 **How to Play LuckyDrop Bot:**\n\n"
        "1.  **Daily Pot:** A new lottery pot opens every day from **5:00 PM to 7:00 PM IST**. 🕰️\n"
        "2.  **Buy a Ticket:** Use `/buyticket` to purchase your lucky entry. Each ticket costs **₹50**. "
        "You can use up to **₹30 from your bonus balance** and the rest from your real balance. Only one ticket per user per day!\n"
        "3.  **Unique Code:** Every ticket has a **unique 6-digit code**.\n"
        "4.  **Wallet:** Check your `real_balance` and `bonus_balance` with `/wallet`. Recharge your real balance manually by paying via the Cashfree link and submitting your payment details for admin approval.\n"
        "5.  **Refer & Earn:** Share your unique referral link (get it with `/refer`). When a friend joins via your link, "
        "starts the bot, and buys their first ticket, you get a **₹10 bonus!** 🤝\n"
        "6.  **Pot Status:** See how many tickets are sold with `/pot`.\n"
        "7.  **UPI Payouts:** Winners get paid directly to their UPI ID. Use `/setupi` to register or update your UPI ID. **Payouts are processed within 12 hours of results!**\n\n"
        "🏆 **Winning Rules (Draw after 7:00 PM IST):**\n"
        "-   **Less than 10 users:** Everyone gets a full refund to their **real** wallet. No hard feelings! ↩️\n"
        "-   **10 to 29 users:** Prizes scale proportionally. The more participants, the bigger the scaled prize! 📈\n"
        "-   **30 users (Full Pot):**\n"
        "   🥇 1st Prize: ₹500\n"
        "   🥈 2nd: ₹200\n"
        "   🥉 3rd: ₹100\n\n"
        "Winners are chosen **fairly and randomly** by the system and announced automatically shortly after the pot closes. Keep an eye out! 👀\n\n"
        "Got it? Let's get lucky! ✨"
    )
    await message.reply(help_text)

@check_channel_membership_decorator
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
        await update_user_upi(db, user_id, new_upi_id)
        await call.message.edit_text(f"🎉 Your UPI ID has been set to: `{escape_markdown_v2(new_upi_id)}`\n"
                                     "You're all set for payouts! 💰", parse_mode='Markdown')
        logger.info(f"User {user_id} confirmed and set UPI ID: {new_upi_id}")
    else:
        await call.message.edit_text("UPI ID not confirmed. Please send your UPI ID again if you wish to set it.")
        await state.set_state(UserStates.WAITING_FOR_UPI_ID)

    await state.clear()