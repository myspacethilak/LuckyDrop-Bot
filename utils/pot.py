import asyncio
from datetime import datetime, time, timedelta
import random
import logging
import pytz
import re
import string
from bson import ObjectId

from db.db_access import update_pot_status, update_user_balance, set_pot_winners, get_user, update_user_upi, add_payout_history, get_pending_payouts_without_upi
from utils.helpers import escape_markdown_v2
from bot_config import (
    DEFAULT_POT_END_HOUR, DEFAULT_POT_START_HOUR, DEFAULT_MAX_USERS, DEFAULT_TICKET_PRICE,
    REVEAL_DELAY_MINUTES, IST_TIMEZONE, UTC_TIMEZONE, ADMIN_ID, MAIN_CHANNEL_ID
)

logger = logging.getLogger(__name__)

# Helper function to generate unique ticket code
def _generate_unique_ticket_code_for_pot(existing_codes):
    while True:
        code = ''.join(random.choices(string.digits, k=6))
        if code not in existing_codes:
            return code

async def create_pot(db, target_date_ist: datetime.date, max_users: int = None, ticket_price: float = None,
                     custom_start_time_ist: datetime = None, custom_end_time_ist: datetime = None):
    if custom_start_time_ist and custom_end_time_ist:
        start_time_ist = custom_start_time_ist
        end_time_ist = custom_end_time_ist
    else:
        start_time_ist = IST_TIMEZONE.localize(datetime.combine(target_date_ist, time(DEFAULT_POT_START_HOUR, 0, 0)))
        end_time_ist = IST_TIMEZONE.localize(datetime.combine(target_date_ist, time(DEFAULT_POT_END_HOUR, 0, 0)))

    pot_data = {
        "date": target_date_ist.isoformat(),
        "start_time": start_time_ist.astimezone(UTC_TIMEZONE),
        "end_time": end_time_ist.astimezone(UTC_TIMEZONE),
        "max_users": max_users if max_users is not None else DEFAULT_MAX_USERS,
        "ticket_price": ticket_price if ticket_price is not None else DEFAULT_TICKET_PRICE,
        "participants": [],
        "total_tickets": 0,
        "status": "open",
        "winners": [],
        "prize_pool": 0.0
    }
    result = await db.pots.insert_one(pot_data)
    pot_data['_id'] = result.inserted_id
    logger.info(f"New pot created for IST date {target_date_ist.isoformat()} (UTC times: {pot_data['start_time']} - {pot_data['end_time']})")

    max_users_for_tickets = max_users if max_users is not None else DEFAULT_MAX_USERS
    tickets_to_insert = []
    generated_codes = set()
    for _ in range(max_users_for_tickets):
        new_code = _generate_unique_ticket_code_for_pot(generated_codes)
        generated_codes.add(new_code)
        tickets_to_insert.append({
            "pot_id": pot_data['_id'],
            "code": new_code,
            "created_at": datetime.now(UTC_TIMEZONE)
        })

    if tickets_to_insert:
        await db.tickets.insert_many(tickets_to_insert)
        logger.info(f"Generated and saved {len(tickets_to_insert)} tickets for pot {pot_data['_id']}")

    return pot_data

async def get_current_pot(db, ist_timezone: pytz.BaseTzInfo):
    today_ist_date_str = datetime.now(ist_timezone).date().isoformat()
    return await db.pots.find_one({
        "date": today_ist_date_str,
        "status": {"$in": ["open", "closed", "revealed"]}
    })

async def process_pot_revelation(bot, db, admin_id: int, pot_data: dict, main_channel_id: int, ist_timezone: pytz.BaseTzInfo, interactive_reveal: bool = False):
    pot_id = pot_data['_id']
    participants = pot_data.get('participants', [])
    num_participants = len(participants)
    ticket_price = pot_data.get('ticket_price', 50.0)
    pot_date_str = pot_data.get('date', 'N/A')

    if pot_data.get('status') == 'revealed':
        if interactive_reveal:
            await bot.send_message(admin_id, "📣 Winners for this pot have already been revealed and prizes distributed.")
        logger.info(f"Pot {pot_id} already revealed. Skipping revelation.")
        return

    if num_participants < 2:
        logger.info(f"Pot {pot_id}: Less than 2 participants ({num_participants}). Refunding all tickets.")
        for participant in participants:
            user_id = participant['telegram_id']
            user_data = await get_user(db, user_id)
            if user_data:
                await update_user_balance(db, user_id, real_amount=ticket_price)
                try:
                    await bot.send_message(user_id, f"😢 Oh no! Today's LuckyDrop pot had less than 2 participants. Your **₹{ticket_price:.2f}** ticket price has been refunded to your real wallet. Better luck next time! 🍀")
                except Exception as e:
                    logger.warning(f"Could not send refund message to user {user_id}: {e}")
            else:
                logger.warning(f"User {user_id} not found for refund in pot {pot_id}.")

        await update_pot_status(db, pot_id, "revealed")
        if main_channel_id:
            try:
                await bot.send_message(main_channel_id, f"😔 Today's LuckyDrop pot ({pot_date_str}) had less than 2 participants ({num_participants} users). All tickets have been refunded! Better luck next time! 🍀")
                logger.info(f"Sent refund announcement to channel {main_channel_id}")
            except Exception as e:
                logger.error(f"Failed to send refund announcement to channel {main_channel_id}: {e}")

        await bot.send_message(admin_id, f"✅ Pot for {pot_date_str} closed due to <2 participants. All tickets refunded. Announcement sent to main channel.")
        logger.info(f"Pot {pot_id} closed and refunds processed.")
        return

    random.shuffle(participants)

    winners_data_for_reveal = []
    base_prizes = {"1st": 500, "2nd": 200, "3rd": 100}

    prizes = {}
    scaled_message = ""
    if num_participants >= 30:
        prizes = base_prizes
    else:
        scaling_factor = num_participants / 30.0
        prizes = {
            "1st": round(base_prizes["1st"] * scaling_factor, 2),
            "2nd": round(base_prizes["2nd"] * scaling_factor, 2),
            "3rd": round(base_prizes["3rd"] * scaling_factor, 2)
        }
        if num_participants >= 10:
            scaled_message = f"Note: With {num_participants} participants, prizes were scaled proportionally from the full pot amount. 📈"

    if num_participants >= 3:
        winners_data_for_reveal.append({"rank": "3rd", "prize": prizes["3rd"], "winner_obj": participants.pop(0)})
        winners_data_for_reveal.append({"rank": "2nd", "prize": prizes["2nd"], "winner_obj": participants.pop(0)})
        winners_data_for_reveal.append({"rank": "1st", "prize": prizes["1st"], "winner_obj": participants.pop(0)})
    elif num_participants == 2:
        winners_data_for_reveal.append({"rank": "2nd", "prize": prizes["2nd"], "winner_obj": participants.pop(0)})
        winners_data_for_reveal.append({"rank": "1st", "prize": prizes["1st"], "winner_obj": participants.pop(0)})
    elif num_participants == 1:
        winners_data_for_reveal.append({"rank": "1st", "prize": prizes["1st"], "winner_obj": participants.pop(0)})

    rank_order_map = {"3rd": 3, "2nd": 2, "1st": 1}
    winners_data_for_reveal.sort(key=lambda x: rank_order_map[x['rank']], reverse=True)

    if interactive_reveal:
        await bot.send_message(chat_id=main_channel_id, text="🥁 **DRUMROLL, PLEASE!** 🥁")
        await asyncio.sleep(2)
        await bot.send_message(chat_id=main_channel_id, text="The winners of today's LuckyDrop pot are being announced now! ✨")
        await asyncio.sleep(2)

    final_winners_for_db = []
    winner_messages_for_summary = []

    for i, rank_info in enumerate(winners_data_for_reveal):
        winner = rank_info["winner_obj"]
        prize = rank_info["prize"]
        rank_name = rank_info["rank"]

        winner_user = await get_user(db, winner['telegram_id'])
        winner_username_display = escape_markdown_v2(winner_user.get('username')) if winner_user and winner_user.get('username') else str(winner['telegram_id'])
        ticket_code = winner['ticket_code']
        winner_upi_id = winner_user.get('upi_id') if winner_user else "Not set"

        if interactive_reveal:
            confetti_gif_url = "https://media.tenor.com/W2P95p_658wAAAAC/confetti-confetti-falling.gif"
            try:
                await bot.send_animation(chat_id=main_channel_id, animation=confetti_gif_url)
            except Exception as e:
                logger.warning(f"Could not send confetti GIF to channel {main_channel_id}. Error: {e}. Sending a text alternative.")
                await bot.send_message(chat_id=main_channel_id, text="🎉✨🥳", parse_mode='Markdown')

            await asyncio.sleep(2)

            reveal_msg_text = f"🏅 The **{rank_name}** prize of **₹{prize:.2f}** goes to [{winner_username_display}](tg://user?id={winner['telegram_id']}) with ticket: `{ticket_code}`! 🎉"
            await bot.send_message(chat_id=main_channel_id, text=reveal_msg_text, parse_mode='Markdown')

            await asyncio.sleep(2)

        final_winners_for_db.append({"rank": rank_name, "telegram_id": winner['telegram_id'], "ticket_code": ticket_code, "prize": prize, "upi_id": winner_upi_id})
        winner_messages_for_summary.append(f"🏅 {rank_name.capitalize()}: [{winner_username_display}](tg://user?id={winner['telegram_id']}) (Ticket `{ticket_code}`) - ₹{prize:.2f} (UPI: `{escape_markdown_v2(winner_upi_id)}`)")

        try:
            if winner_user and winner_upi_id != "Not set":
                winner_message_text = (
                    f"🥳 **CONGRATULATIONS, YOU WON!** 🎉\n\n"
                    f"You've secured the **{rank_name}** prize of **₹{prize:.2f}** in today's LuckyDrop! 🏆\n"
                    f"Your winnings will be sent directly to your registered UPI ID within 12 hours. Best of luck for the next round! 🚀"
                )
            else:
                winner_message_text = (
                    f"🥳 **CONGRATULATIONS, YOU WON!** 🎉\n\n"
                    f"You've secured the **{rank_name}** prize of **₹{prize:.2f}** in today's LuckyDrop! 🏆\n"
                    f"But you haven't set your UPI ID yet! Please use `/setupi` within the next 10 hours to register your UPI ID so we can send your winnings. Best of luck for the next round! 🚀"
                )
            await bot.send_message(winner['telegram_id'], winner_message_text, parse_mode='Markdown')
        except Exception as e:
            logger.warning(f"Could not send winner message to {winner['telegram_id']}: {e}")

        try:
            admin_payout_message = (
                f"🚨 **PAYOUT ALERT!** 🚨\n\n"
                f"**Winner:** [{winner_username_display}](tg://user?id={winner['telegram_id']})\n"
                f"**Prize:** ₹{prize:.2f}\n"
                f"**UPI ID:** `{escape_markdown_v2(winner_upi_id)}`\n"
                f"**Pot ID:** `{str(pot_id)}`\n"
                f"**Status:** PENDING\n\n"
                f"Please process this payout manually."
            )
            await bot.send_message(admin_id, admin_payout_message, parse_mode='Markdown')
            logger.info(f"Admin notified about payout for winner {winner['telegram_id']}.")
        except Exception as e:
            logger.error(f"Failed to send admin payout message: {e}")

        await add_payout_history(db, winner['telegram_id'], pot_id, prize, "PENDING", winner_upi_id)

        if interactive_reveal:
            await asyncio.sleep(2)

    final_winners_sorted_for_db = sorted(final_winners_for_db, key=lambda x: rank_order_map[x['rank']])
    await set_pot_winners(db, pot_id, final_winners_sorted_for_db)
    await update_pot_status(db, pot_id, "revealed")

    final_announcement_lines = [
        "🏆 **TODAY'S LUCKY WINNERS ARE:** 🏆",
        ""
    ]
    if scaled_message:
        final_announcement_lines.append(scaled_message)
        final_announcement_lines.append("")

    public_winner_messages = [re.sub(r' \(UPI: `.*?`\)', '', msg) for msg in winner_messages_for_summary]
    final_announcement_lines.extend(public_winner_messages)
    final_announcement_lines.extend([
        "",
        "🥳 Congratulations to all our winners! Better luck next time for everyone else! ✨",
        "A new pot opens daily at 5:00 PM IST. Get ready! 🚀",
        "Payouts will be processed to registered UPI IDs within 12 hours. Please ensure your UPI ID is set with /setupi. Good luck for next round!"
    ])
    final_announcement = "\n".join(final_announcement_lines)
    await bot.send_message(chat_id=main_channel_id, text=final_announcement, parse_mode='Markdown')

    logger.info(f"Pot {pot_id} revelation completed.")

# FIX: The scheduler logic is now dynamic and checks the pot's end_time
async def schedule_daily_pot_open(bot, db, admin_id: int, main_channel_id: int, ist_timezone: pytz.BaseTzInfo, utc_timezone: pytz.BaseTzInfo):
    logger.info(f"Pot scheduler started. Default pot times: {DEFAULT_POT_START_HOUR}:00 - {DEFAULT_POT_END_HOUR}:00 IST")
    while True:
        now_ist = datetime.now(ist_timezone)
        today_ist_date = now_ist.date()
        today_ist_date_str = today_ist_date.isoformat()
        pot_open_time_default_ist = ist_timezone.localize(datetime.combine(today_ist_date, time(DEFAULT_POT_START_HOUR, 0, 0)))

        current_pot = await get_current_pot(db, ist_timezone)

        if current_pot:
            pot_end_time_ist = current_pot.get('end_time').astimezone(ist_timezone)
            current_pot_status = current_pot.get('status')

            # Case 1: Pot is open and it's past its end time
            if current_pot_status == 'open' and now_ist >= pot_end_time_ist:
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is OPEN and it's past pot's end time. Closing pot.")
                try:
                    await bot.send_message(main_channel_id,
                                           "⏳ **Time's up!** The LuckyDrop pot is now closed for ticket purchases! Results coming soon! 🎲",
                                           parse_mode='Markdown')
                    logger.info(f"Sent pot close announcement to channel {main_channel_id}")
                except Exception as e:
                    logger.error(f"Failed to send pot close announcement to channel {main_channel_id}: {e}")
                await close_pot_and_distribute_prizes(bot, db, admin_id, current_pot['_id'], main_channel_id=main_channel_id)
                await asyncio.sleep(60) # Wait a minute before checking again

            # Case 2: Pot is closed and it's past the reveal time
            elif current_pot_status == 'closed' and now_ist >= pot_end_time_ist + timedelta(minutes=REVEAL_DELAY_MINUTES):
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is CLOSED and it's past auto-reveal time. Triggering auto-revelation.")
                await process_pot_revelation(bot, db, admin_id, current_pot, main_channel_id, ist_timezone, interactive_reveal=False)
                await asyncio.sleep(3600) # Wait for an hour after reveal

            # Case 3: Pot is already revealed, wait for tomorrow
            elif current_pot_status == 'revealed':
                logger.debug(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is already revealed. Waiting for tomorrow's pot.")
                next_5pm_tomorrow_ist = ist_timezone.localize(datetime.combine(today_ist_date + timedelta(days=1), time(DEFAULT_POT_START_HOUR, 0, 0)))
                sleep_seconds = (next_5pm_tomorrow_ist - now_ist).total_seconds()
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Sleeping for {sleep_seconds / 3600:.2f} hours until tomorrow's {DEFAULT_POT_START_HOUR} PM IST.")
                await asyncio.sleep(max(60, sleep_seconds))

            # Case 4: Pot is open, but it's not time to close yet
            else:
                logger.debug(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is open. Checking again in 1 min.")
                await asyncio.sleep(60)

        # Case 5: No pot exists for today yet
        else:
            if now_ist >= pot_open_time_default_ist:
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Current time is ON/AFTER 5PM IST and no pot exists. Creating a new one.")
                new_pot = await create_pot(db, today_ist_date)
                if new_pot:
                    try:
                        await bot.send_message(main_channel_id,
                                               "🔔 **POT ALERT!** A new LuckyDrop pot is now open for tickets! 🚀\n"
                                               "Time to grab your ticket before 7:00 PM IST! Use /buyticket now! 🎫",
                                               parse_mode='Markdown')
                        logger.info(f"Sent auto pot open announcement to channel {main_channel_id}")
                    except Exception as e:
                        logger.error(f"Failed to send auto pot open announcement to channel {main_channel_id}: {e}")
                    await bot.send_message(admin_id, f"🔔 Pot auto-opened for {today_ist_date_str}. Announcement sent to main channel. 🚀")
                await asyncio.sleep(600)
            else:
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Current time is BEFORE 5PM IST. Waiting for pot opening.")
                next_check_time_ist = pot_open_time_default_ist
                sleep_seconds = (next_check_time_ist - now_ist).total_seconds()
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Sleeping for {sleep_seconds / 3600:.2f} hours until {DEFAULT_POT_START_HOUR} PM IST.")
                await asyncio.sleep(max(60, sleep_seconds))

async def send_payout_reminders(bot, db):
    """
    Background task to send a reminder to winners without a set UPI ID.
    """
    while True:
        logger.info("Running payout reminder task...")
        winners_to_remind = await db.payouts.find({
            "status": "PENDING",
            "upi_id": "Not set"
        }).to_list(length=None)

        for winner_data in winners_to_remind:
            user_id = winner_data['user_telegram_id']
            prize_amount = winner_data['amount']

            try:
                reminder_message = (
                    f"⏰ **URGENT REMINDER:** You have a pending prize of **₹{prize_amount:.2f}** waiting for you! 🏆\n"
                    "We can't send your winnings because you haven't set your UPI ID.\n\n"
                    "Please use the command `/setupi` to register your UPI ID and claim your prize. You have a limited time to do so. Don't miss out! 🚀"
                )
                await bot.send_message(user_id, reminder_message, parse_mode='Markdown')
                logger.info(f"Sent UPI reminder to winner {user_id}")
            except Exception as e:
                logger.warning(f"Could not send UPI reminder message to winner {user_id}: {e}")

        await asyncio.sleep(3600)

async def close_pot_and_distribute_prizes(bot, db, admin_id: int, pot_id, main_channel_id: int = None):
    current_pot = await db.pots.find_one({"_id": pot_id})
    if not current_pot:
        logger.error(f"Pot with ID {pot_id} not found for closing.")
        return
    participants = current_pot.get('participants', [])
    num_participants = len(participants)
    ticket_price = current_pot.get('ticket_price', 50.0)
    if current_pot.get('status') != 'open':
        logger.info(f"Pot {pot_id} is already '{current_pot.get('status')}'. No action needed for closing.")
        return

    all_tickets_in_pot = await db.tickets.find({"pot_id": pot_id}).to_list(length=None)
    sold_ticket_codes = {p['ticket_code'] for p in current_pot.get('participants', [])}

    ticket_list_message = "Here are all the tickets for today's pot! See which ones were sold.\n\n"
    ticket_grid = []
    row = []

    for ticket in all_tickets_in_pot:
        if ticket['code'] in sold_ticket_codes:
            row.append(f"🔒 `{ticket['code']}`")
        else:
            row.append(f"🍀 `{ticket['code']}`")

        if len(row) == 5:
            ticket_grid.append(" ".join(row))
            row = []
    if row:
        ticket_grid.append(" ".join(row))

    ticket_list_message += "\n".join(ticket_grid)

    if num_participants < 2:
        logger.info(f"Pot {pot_id}: Less than 2 participants ({num_participants}). Proceeding to refund all tickets.")
        await update_pot_status(db, pot_id, "closed")
        if main_channel_id:
            try:
                await bot.send_message(main_channel_id,
                                       "😔 Today's LuckyDrop pot had less than 2 participants. All tickets will be refunded automatically shortly! 🍀\n\n" + ticket_list_message,
                                       parse_mode='Markdown')
                logger.info(f"Sent pre-refund announcement to channel {main_channel_id}")
            except Exception as e:
                logger.error(f"Failed to send pre-refund announcement to channel {main_channel_id}: {e}")
        await bot.send_message(admin_id, f"✅ Pot for {current_pot['date']} has officially closed with <2 participants. Refunds will be processed and announced automatically in {REVEAL_DELAY_MINUTES} minutes.")
        logger.info(f"Pot {pot_id} status set to 'closed' for refund processing.")
    else:
        logger.info(f"Pot {pot_id}: {num_participants} participants. Marking as closed for admin/auto reveal.")
        await update_pot_status(db, pot_id, "closed")
        if main_channel_id:
            try:
                await bot.send_message(main_channel_id,
                                       f"🎉 **Pot Closed!** Today's LuckyDrop pot has **{num_participants} participants**!\n\n" + ticket_list_message,
                                       parse_mode='Markdown')

                await asyncio.sleep(5)

                await bot.send_message(main_channel_id,
                                       f"Winners will be announced automatically in {REVEAL_DELAY_MINUTES} minutes! Stay tuned! 🏆",
                                       parse_mode='Markdown')
                logger.info(f"Sent pot ready for reveal announcement to channel {main_channel_id}")
            except Exception as e:
                logger.error(f"Failed to send pot ready for reveal announcement to channel {main_channel_id}: {e}")
        await bot.send_message(admin_id,
                               f"🔔 **Pot Closed for Revelation!**\n"
                               f"Today's LuckyDrop pot ({current_pot['date']}) has **{num_participants} participants**.\n"
                               f"Winners will be automatically revealed and prizes distributed in {REVEAL_DELAY_MINUTES} minutes. You can also use /reveal manually.")
        logger.info(f"Pot {pot_id} closed, awaiting admin or auto reveal.")

async def get_current_pot_status(pot_data, detailed: bool = False, db=None, ist_timezone: pytz.BaseTzInfo = None):
    if not pot_data:
        return "No active pot at the moment. ⏳"
    filled_count = len(pot_data.get('participants', []))
    max_users = pot_data.get('max_users', 30)
    ticket_price = pot_data.get('ticket_price', 50.0)
    status = pot_data.get('status', 'unknown')
    status_text = {
        "open": "🟢 OPEN for tickets!",
        "closed": "🔴 CLOSED for tickets. Awaiting winner reveal!",
        "revealed": "✨ REVEALED! Winners announced!",
        "unknown": "❓ Unknown status"
    }.get(status, "❓ Unknown status")
    start_time_display = "N/A"
    end_time_display = "N/A"
    if pot_data.get('start_time') and isinstance(pot_data['start_time'], datetime):
        if pot_data['start_time'].tzinfo is None:
            start_time_utc = pytz.utc.localize(pot_data['start_time'])
        else:
            start_time_utc = pot_data['start_time']
        start_time_display = start_time_utc.astimezone(ist_timezone).strftime('%I:%M %p') if ist_timezone else start_time_utc.strftime('%I:%M %p %Z')
    if pot_data.get('end_time') and isinstance(pot_data['end_time'], datetime):
        if pot_data['end_time'].tzinfo is None:
            end_time_utc = pytz.utc.localize(pot_data['end_time'])
        else:
            end_time_utc = pot_data['end_time']
        end_time_display = end_time_utc.astimezone(ist_timezone).strftime('%I:%M %p') if ist_timezone else end_time_utc.strftime('%I:%M %p %Z')
    message = (
        f"📅 Pot Date: {pot_data['date']}\n"
        f"⏰ Time Window: {start_time_display} - {end_time_display} IST\n"
        f"🎟️ Tickets Sold: **{filled_count}/{max_users}** ({status_text})\n"
        f"💸 Ticket Price: ₹{ticket_price:.2f}"
    )
    if detailed and db is not None:
        message += (
            f"\n\n**Pot ID:** `{str(pot_data['_id'])}`\n"
            f"**Current Participants:**\n"
        )
        if filled_count > 0:
            for participant in pot_data['participants']:
                user_obj = await get_user(db, participant['telegram_id'])
                username = escape_markdown_v2(user_obj.get('username')) if user_obj and user_obj.get('username') else f"User {participant['telegram_id']}"
                message += f"- {username} (ID: {participant['telegram_id']}) - Ticket: `{participant['ticket_code']}`\n"
        else:
            message += "- _No participants yet._"
        if status == 'revealed' and pot_data.get('winners'):
            message += "\n\n**🏆 Winners:**\n"
            rank_order_map = {"3rd": 3, "2nd": 2, "1st": 1}
            display_winners = sorted(pot_data['winners'], key=lambda x: rank_order_map[x['rank']])
            for winner in display_winners:
                winner_user_obj = await get_user(db, winner['telegram_id'])
                winner_username = escape_markdown_v2(winner_user_obj.get('username')) if winner_user_obj and winner_user_obj.get('username') else f"User {winner['telegram_id']}"
                winner_upi_id = escape_markdown_v2(winner.get('upi_id', 'Not Set'))
                message += f"- {winner['rank']}: [{winner_username}](tg://user?id={winner['telegram_id']}) (Ticket: `{winner['ticket_code']}`) - Prize: ₹{winner['prize']:.2f} (UPI: `{winner_upi_id}`)\n"
    return message