import asyncio
from datetime import datetime, time, timedelta
import random
import logging
import pytz
import re

from db.db_access import update_pot_status, update_user_balance, set_pot_winners, get_user, update_user_upi
from utils.helpers import escape_markdown_v2
from bot_config import (
    DEFAULT_POT_END_HOUR, DEFAULT_POT_START_HOUR, DEFAULT_MAX_USERS, DEFAULT_TICKET_PRICE,
    REVEAL_DELAY_MINUTES, IST_TIMEZONE, UTC_TIMEZONE, ADMIN_ID, MAIN_CHANNEL_ID
)

logger = logging.getLogger(__name__)


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

    if num_participants < 10:
        logger.info(f"Pot {pot_id}: Less than 10 participants ({num_participants}). Refunding all tickets.")
        for participant in participants:
            user_id = participant['telegram_id']
            user_data = await get_user(db, user_id)
            if user_data:
                await update_user_balance(db, user_id, real_amount=ticket_price)
                try:
                    await bot.send_message(user_id, f"😢 Oh no! Today's LuckyDrop pot had less than 10 participants. Your **₹{ticket_price:.2f}** ticket price has been refunded to your real wallet. Better luck next time! 🍀")
                except Exception as e:
                    logger.warning(f"Could not send refund message to user {user_id}: {e}")
            else:
                logger.warning(f"User {user_id} not found for refund in pot {pot_id}.")

        await update_pot_status(db, pot_id, "revealed")
        if main_channel_id:
            try:
                await bot.send_message(main_channel_id, f"😔 Today's LuckyDrop pot ({pot_date_str}) had less than 10 participants ({num_participants} users). All tickets have been refunded! Better luck next time! 🍀")
                logger.info(f"Sent refund announcement to channel {main_channel_id}")
            except Exception as e:
                logger.error(f"Failed to send refund announcement to channel {main_channel_id}: {e}")

        await bot.send_message(admin_id, f"✅ Pot for {pot_date_str} closed due to <10 participants. All tickets refunded. Announcement sent to main channel.")
        logger.info(f"Pot {pot_id} closed and refunds processed.")
        return

    random.shuffle(participants)

    winners_data_for_reveal = []
    base_prizes = {"1st": 500, "2nd": 200, "3rd": 100}

    prizes = {}
    if num_participants >= 30:
        prizes = base_prizes
    else:
        scaling_factor = num_participants / 30.0
        prizes = {
            "1st": round(base_prizes["1st"] * scaling_factor, 2),
            "2nd": round(base_prizes["2nd"] * scaling_factor, 2),
            "3rd": round(base_prizes["3rd"] * scaling_factor, 2)
        }

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
        await bot.send_message(chat_id=admin_id, text="🥁 Drumroll, please! The LuckyDrop results are in! 🥁", parse_mode='Markdown')
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
            reveal_msg_text = f"🌟 Now for the **{rank_name}** prize... The winning ticket number is: "
            sent_message = await bot.send_message(chat_id=admin_id, text=reveal_msg_text + "`______`", parse_mode='Markdown')
            revealed_part = ""
            for j, digit in enumerate(ticket_code):
                revealed_part += digit
                await asyncio.sleep(0.7)
                await bot.edit_message_text(chat_id=admin_id, message_id=sent_message.message_id,
                                            text=reveal_msg_text + f"`{revealed_part}{'_' * (len(ticket_code) - j - 1)}`",
                                            parse_mode='Markdown')
            await asyncio.sleep(1)
            await bot.edit_message_text(chat_id=admin_id, message_id=sent_message.message_id,
                                        text=f"🏅 The **{rank_name}** prize of **₹{prize:.2f}** goes to [{winner_username_display}](tg://user?id={winner['telegram_id']}) with ticket: `{ticket_code}`! 🎉",
                                        parse_mode='Markdown')
        final_winners_for_db.append({"rank": rank_name, "telegram_id": winner['telegram_id'], "ticket_code": ticket_code, "prize": prize, "upi_id": winner_upi_id})
        winner_messages_for_summary.append(f"🏅 {rank_name.capitalize()}: [{winner_username_display}](tg://user?id={winner['telegram_id']}) (Ticket `{ticket_code}`) - ₹{prize:.2f} (UPI: `{escape_markdown_v2(winner_upi_id)}`)")
        try:
            winner_message_text = (
                f"🥳 **YOU WON!** Congratulations, you've won **₹{prize:.2f}** as the {rank_name} prize in the LuckyDrop Bot!\n"
                f"Your winnings will be transferred directly to your registered UPI ID "
            )
            if winner_upi_id and winner_upi_id != "Not set":
                winner_message_text += f"`{escape_markdown_v2(winner_upi_id)}` within 12 hours. 🎉"
            else:
                winner_message_text += "❗️ **Important:** You have not registered a UPI ID yet!\n" \
                                       "Please set your UPI ID using the `/setupi` command so we can send your winnings. " \
                                       "Payouts are processed within 12 hours. 🚀"
            await bot.send_message(winner['telegram_id'], winner_message_text, parse_mode='Markdown')
        except Exception as e:
            logger.warning(f"Could not send winner message to {winner['telegram_id']}: {e}")
        if interactive_reveal:
            await asyncio.sleep(2)
    final_winners_sorted_for_db = sorted(final_winners_for_db, key=lambda x: rank_order_map[x['rank']])
    await set_pot_winners(db, pot_id, final_winners_sorted_for_db)
    await update_pot_status(db, pot_id, "revealed")
    final_announcement_lines = [
        "🏆 **TODAY'S LUCKY WINNERS ARE:** 🏆",
        ""
    ]
    final_announcement_lines.extend(winner_messages_for_summary)
    final_announcement_lines.extend([
        "",
        "🥳 Congratulations to all our winners! Better luck next time for everyone else! ✨",
        "A new pot opens daily at 5:00 PM IST. Get ready! 🚀",
        "UPI payouts will be processed within 12 hours."
    ])
    final_announcement = "\n".join(final_announcement_lines)
    await bot.send_message(chat_id=admin_id, text=final_announcement, parse_mode='Markdown')
    if main_channel_id:
        channel_announcement_lines = [
            f"🎉 **LUCKYDROP RESULTS FOR {pot_date_str} ARE IN!** 🎉",
            "",
            f"🏆 **Winners (Chosen Fairly & Randomly):**",
        ]
        channel_summary_for_public = []
        for winner_msg in winner_messages_for_summary:
            channel_summary_for_public.append(re.sub(r' \(UPI: `.*?`\)', '', winner_msg))
        channel_announcement_lines.extend(channel_summary_for_public)
        channel_announcement_lines.extend([
            "",
            "🥳 Congratulations to all our winners! A new pot opens daily at 5:00 PM IST. Get ready! 🚀",
            "All payouts will be processed to registered UPI IDs within 12 hours. Please ensure your UPI ID is set with /setupi. Good luck for next round!"
        ])
        channel_announcement = "\n".join(channel_announcement_lines)
        try:
            await bot.send_message(main_channel_id, channel_announcement, parse_mode='Markdown')
            logger.info(f"Sent reveal announcement to channel {main_channel_id}")
        except Exception as e:
            logger.error(f"Failed to send reveal announcement to channel {main_channel_id}: {e}")
    logger.info(f"Pot {pot_id} revelation completed.")
async def schedule_daily_pot_open(bot, db, admin_id: int, main_channel_id: int, ist_timezone: pytz.BaseTzInfo, utc_timezone: pytz.BaseTzInfo):
    logger.info(f"Pot scheduler started. Default pot times: {DEFAULT_POT_START_HOUR}:00 - {DEFAULT_POT_END_HOUR}:00 IST")
    while True:
        now_ist = datetime.now(ist_timezone)
        today_ist_date = now_ist.date()
        today_ist_date_str = today_ist_date.isoformat()
        pot_open_time_ist = ist_timezone.localize(datetime.combine(today_ist_date, time(DEFAULT_POT_START_HOUR, 0, 0)))
        pot_close_time_ist = ist_timezone.localize(datetime.combine(today_ist_date, time(DEFAULT_POT_END_HOUR, 0, 0)))
        pot_auto_reveal_time_ist = pot_close_time_ist + timedelta(minutes=REVEAL_DELAY_MINUTES)
        current_pot = await get_current_pot(db, ist_timezone)
        current_pot_status = current_pot.get('status') if current_pot else "None"
        current_pot_db_date = current_pot.get('date') if current_pot else "None"
        logger.info(f"[{now_ist.strftime('%Y-%m-%d %H:%M:%S %Z%z')}] Scheduler Loop: Pot Status for {today_ist_date_str}: {current_pot_status}")
        if now_ist >= pot_open_time_ist and now_ist < pot_close_time_ist:
            logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Current time is WITHIN 5PM-7PM IST window.")
            if not current_pot or current_pot_db_date != today_ist_date_str or current_pot_status == 'revealed' or current_pot_status == 'closed':
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is not 'open' (status: {current_pot_status}). Attempting to create/re-open.")
                if current_pot and current_pot_db_date == today_ist_date_str:
                    await db.pots.delete_one({"date": today_ist_date_str})
                    logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Deleted old pot (status: {current_pot_status}) for {today_ist_date_str} to create a fresh auto-opened pot.")
                new_pot = await create_pot(db, today_ist_date)
                if new_pot:
                    try:
                        await bot.send_message(main_channel_id,
                                               "🔔 **POT ALERT!** A new LuckyDrop pot is now open for tickets! 🚀\n"
                                               "Time to grab your ticket before 7:00 PM IST! Use /buyticket now! 🎫")
                        logger.info(f"Sent auto pot open announcement to channel {main_channel_id}")
                    except Exception as e:
                        logger.error(f"Failed to send auto pot open announcement to channel {main_channel_id}: {e}")
                    await bot.send_message(admin_id, f"🔔 Pot auto-opened for {today_ist_date_str}. Announcement sent to main channel. 🚀")
                await asyncio.sleep(600)
            else:
                logger.debug(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is already open. Sleeping for 10 min.")
                await asyncio.sleep(600)
        elif now_ist >= pot_close_time_ist:
            logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Current time is ON/AFTER 7PM IST. Checking pot closure and revelation.")
            if current_pot and current_pot_db_date == today_ist_date_str and current_pot_status == 'open' and now_ist >= current_pot['end_time'].astimezone(ist_timezone):
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is OPEN and it's past pot's end time. Closing pot.")
                try:
                    await bot.send_message(main_channel_id,
                                           "⏳ **Time's up!** The LuckyDrop pot is now closed for ticket purchases! Results coming soon! 🎲")
                    logger.info(f"Sent pot close announcement to channel {main_channel_id}")
                except Exception as e:
                    logger.error(f"Failed to send pot close announcement to channel {main_channel_id}: {e}")
                await close_pot_and_distribute_prizes(bot, db, admin_id, current_pot['_id'], main_channel_id=main_channel_id)
                await asyncio.sleep(30)
            current_pot_after_close = await get_current_pot(db, ist_timezone)
            if current_pot_after_close and current_pot_after_close.get('date') == today_ist_date_str:
                if current_pot_after_close.get('status') == 'closed' and now_ist >= current_pot_after_close['end_time'].astimezone(ist_timezone) + timedelta(minutes=REVEAL_DELAY_MINUTES):
                    logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is CLOSED and it's past auto-reveal time. Triggering auto-revelation.")
                    await process_pot_revelation(bot, db, admin_id, current_pot_after_close, main_channel_id, ist_timezone, interactive_reveal=False)
                    await asyncio.sleep(3600)
                elif current_pot_after_close.get('status') == 'closed':
                    logger.debug(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is closed. Awaiting auto-reveal (in {(current_pot_after_close['end_time'].astimezone(ist_timezone) + timedelta(minutes=REVEAL_DELAY_MINUTES) - now_ist).total_seconds()/60:.1f} min) or admin manual reveal. Sleeping for 1 min.")
                    await asyncio.sleep(60)
                elif current_pot_after_close.get('status') == 'revealed':
                    logger.debug(f"[{now_ist.strftime('%H:%M:%S %Z')}] Pot for {today_ist_date_str} is already revealed. Waiting for tomorrow's pot.")
                    next_5pm_tomorrow_ist = ist_timezone.localize(datetime.combine(today_ist_date + timedelta(days=1), time(DEFAULT_POT_START_HOUR, 0, 0)))
                    sleep_seconds = (next_5pm_tomorrow_ist - now_ist).total_seconds()
                    logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Sleeping for {sleep_seconds / 3600:.2f} hours until tomorrow's 5 PM IST.")
                    await asyncio.sleep(max(60, sleep_seconds + 60))
            else:
                next_5pm_tomorrow_ist = ist_timezone.localize(datetime.combine(today_ist_date + timedelta(days=1), time(DEFAULT_POT_START_HOUR, 0, 0)))
                sleep_seconds = (next_5pm_tomorrow_ist - now_ist).total_seconds()
                logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Past 7 PM IST and no pot for today. Sleeping for {sleep_seconds / 3600:.2f} hours until tomorrow's 5 PM IST.")
                await asyncio.sleep(max(60, sleep_seconds + 60))
        else:
            logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Current time is BEFORE 5PM IST. Waiting for pot opening.")
            next_check_time_ist = pot_open_time_ist
            sleep_seconds = (next_check_time_ist - now_ist).total_seconds()
            logger.info(f"[{now_ist.strftime('%H:%M:%S %Z')}] Sleeping for {sleep_seconds / 3600:.2f} hours until 5 PM IST.")
            await asyncio.sleep(max(60, sleep_seconds + 60))
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
    if num_participants < 10:
        logger.info(f"Pot {pot_id}: Less than 10 participants ({num_participants}). Proceeding to refund all tickets.")
        await update_pot_status(db, pot_id, "closed")
        if main_channel_id:
            try:
                await bot.send_message(main_channel_id, f"😔 Today's LuckyDrop pot had less than 10 participants ({num_participants} users). All tickets will be refunded automatically shortly! 🍀")
                logger.info(f"Sent pre-refund announcement to channel {main_channel_id}")
            except Exception as e:
                logger.error(f"Failed to send pre-refund announcement to channel {main_channel_id}: {e}")
        await bot.send_message(admin_id, f"✅ Pot for {current_pot['date']} has officially closed with <10 participants. Refunds will be processed and announced automatically in {REVEAL_DELAY_MINUTES} minutes.")
        logger.info(f"Pot {pot_id} status set to 'closed' for refund processing.")
    else:
        logger.info(f"Pot {pot_id}: {num_participants} participants. Marking as closed for admin/auto reveal.")
        await update_pot_status(db, pot_id, "closed")
        if main_channel_id:
            try:
                await bot.send_message(main_channel_id,
                                       f"🎉 **Pot Closed!** Today's LuckyDrop pot has **{num_participants} participants**! "
                                       f"Winners will be announced automatically in {REVEAL_DELAY_MINUTES} minutes! Stay tuned! 🏆")
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