import os
import asyncio
import logging
from functools import partial
from collections import defaultdict
import json

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, filters,
    ContextTypes, ConversationHandler
)

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# ==== CONFIG ====
BOT_TOKEN = "7675152993:AAFNHoEleddTmMBqutwI5k32l-364iC2BmU"
API_ID = 23441916      # int!
API_HASH = "441dab9c9a15ecb4af91e8aa4f830f7e"

# Admin Telegram ID (replace with your ID)
ADMIN_ID = 6503507032  # Change this to your Telegram ID

SESSION_DIR = "sessions"
USER_DATA_DIR = "user_data"
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(USER_DATA_DIR, exist_ok=True)

# Set up logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==== GLOBALS ====
(
    MENU, ADD_GROUP, REMOVE_GROUP, MANAGE_MESSAGE, ADD_ACCOUNT, REMOVE_ACCOUNT,
    MANAGE_INTERVAL, ADD_ACCOUNT_PHONE, ADD_ACCOUNT_CODE, ADMIN_MENU, APPROVE_USER
) = range(11)
# Structure: {user_id: {"phone": str, "phone_code_hash": str}}
pending_phones = {}

# Structure: {user_id: {"groups": [], "ad_message": str, "post_interval": int, "posting": bool}}
user_data = {}

# Structure: {user_id: {"approved": bool, "is_admin": bool}}
user_permissions = {}

# Load user permissions from file
def load_permissions():
    global user_permissions
    try:
        with open('user_permissions.json', 'r') as f:
            user_permissions = json.load(f)
        # Ensure admin is always in permissions
        if str(ADMIN_ID) not in user_permissions:
            user_permissions[str(ADMIN_ID)] = {"approved": True, "is_admin": True}
            save_permissions()
    except FileNotFoundError:
        # Initialize with admin
        user_permissions = {str(ADMIN_ID): {"approved": True, "is_admin": True}}
        save_permissions()

def save_permissions():
    with open('user_permissions.json', 'w') as f:
        json.dump(user_permissions, f)

# Load user data
def load_user_data(user_id):
    user_id_str = str(user_id)
    try:
        with open(f'{USER_DATA_DIR}/{user_id_str}.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        # Default user data
        return {
            "groups": [],
            "ad_message": "This is a default ad message. Use 'Manage Message' to change.",
            "post_interval": 30,
            "posting": False
        }

def save_user_data(user_id, data):
    user_id_str = str(user_id)
    with open(f'{USER_DATA_DIR}/{user_id_str}.json', 'w') as f:
        json.dump(data, f)

# Cache for TelegramClient instances
client_cache = {}  # phone -> client
client_last_used = {}  # phone -> timestamp

main_menu_keyboard = [
    ["Manage Groups", "Manage Message"],
    ["Manage Accounts", "Manage Interval"],
    ["Start Posting", "Stop Posting"]
]
main_menu_markup = ReplyKeyboardMarkup(main_menu_keyboard, resize_keyboard=True)

admin_menu_keyboard = [
    ["List Users", "Approve Users"],
    ["Revoke Access", "Back"]
]
admin_menu_markup = ReplyKeyboardMarkup(admin_menu_keyboard, resize_keyboard=True)

# ==== HELPER FUNCTIONS ====
def is_admin(user_id):
    user_id_str = str(user_id)
    return user_id_str in user_permissions and user_permissions[user_id_str].get("is_admin", False)

def is_approved(user_id):
    user_id_str = str(user_id)
    return user_id_str in user_permissions and user_permissions[user_id_str].get("approved", False)

async def check_access(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_approved(user_id):
        await update.message.reply_text(
            "You are not approved to use this bot. Please contact the admin.",
            reply_markup=ReplyKeyboardRemove()
        )
        return False
    return True

# ==== TELETHON HELPERS ====
def get_all_sessions(user_id):
    user_sessions = []
    for f in os.listdir(SESSION_DIR):
        if f.endswith('.session') and f.startswith(f"{user_id}_"):
            user_sessions.append(f.replace('.session', '').split('_', 1)[1])
    return user_sessions

async def get_client(user_id, phone):
    """Get or create a TelegramClient for a phone number"""
    cache_key = f"{user_id}_{phone}"
    if cache_key in client_cache and client_cache[cache_key].is_connected():
        client_last_used[cache_key] = asyncio.get_event_loop().time()
        return client_cache[cache_key]
    
    session_path = os.path.join(SESSION_DIR, f"{user_id}_{phone}")
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        await client.connect()
        client_cache[cache_key] = client
        client_last_used[cache_key] = asyncio.get_event_loop().time()
    except Exception as e:
        logger.error(f"Error connecting client for {phone}: {e}")
        await client.disconnect()
        return None
        
    return client

async def cleanup_idle_clients(max_idle_time=300):  # 5 minutes
    """Clean up idle clients"""
    current_time = asyncio.get_event_loop().time()
    keys_to_remove = []
    
    for key, last_used in client_last_used.items():
        if current_time - last_used > max_idle_time:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        try:
            if key in client_cache:
                client = client_cache[key]
                if client.is_connected():
                    await client.disconnect()
                del client_cache[key]
            del client_last_used[key]
            logger.info(f"Cleaned up idle client for {key}")
        except Exception as e:
            logger.error(f"Error cleaning up client for {key}: {e}")

async def send_message_as_account(client, group, message, retries=2):
    """Send message using a specific client with retries"""
    for attempt in range(retries + 1):
        try:
            if not client.is_connected():
                await client.connect()
            
            await client.send_message(group, message)
            return True, f"Sent successfully"
        except FloodWaitError as e:
            wait_time = e.seconds
            logger.warning(f"FloodWaitError: Need to wait {wait_time} seconds")
            return False, f"Rate limited (wait {wait_time}s)"
        except Exception as e:
            if attempt < retries:
                logger.warning(f"Failed to send, retrying ({attempt+1}/{retries}): {e}")
                await asyncio.sleep(1)
            else:
                logger.error(f"Failed to send after {retries} retries: {e}")
                return False, f"Failed: {str(e)[:50]}..."
    
    return False, "Unknown error"

async def send_message_to_group(user_id, message, group, phone):
    """Send message to a group using specified account"""
    client = await get_client(user_id, phone)
    if not client:
        return False, "Client not available"
        
    try:
        if not await client.is_user_authorized():
            return False, "Account not authorized"
            
        ok, msg = await send_message_as_account(client, group, message)
        return ok, msg
    except Exception as e:
        logger.error(f"Error with account {phone}: {e}")
        return False, str(e)
    # Try accounts in rotation
    for phone in sessions:
        client = await get_client(user_id, phone)
        if not client:
            continue
            
        try:
            if not await client.is_user_authorized():
                logger.warning(f"Account {phone} is not authorized")
                continue
                
            ok, msg = await send_message_as_account(client, group, message)
            if ok:
                return True, f"Sent as {phone}"
        except Exception as e:
            logger.error(f"Error with account {phone}: {e}")
            
    return False, "Failed to send from all accounts."

# ==== POSTING LOOP ====
async def post_to_groups(user_id, context: ContextTypes.DEFAULT_TYPE):
    user_data = load_user_data(user_id)
    user_data["posting"] = True
    save_user_data(user_id, user_data)
    
    error_counts = defaultdict(int)
    max_errors = 3
    account_rotation = defaultdict(int)  # Track account index per group
    last_post_time = defaultdict(float)  # Track last post time per group
    
    # Initialize staggered posting times
    groups = user_data["groups"]
    interval = user_data["post_interval"]
    current_time = asyncio.get_event_loop().time()
    
    # Stagger initial post times
    for idx, group in enumerate(groups):
        last_post_time[group] = current_time - interval + (idx * (interval / len(groups)))
    
    while user_data["posting"]:
        user_data = load_user_data(user_id)
        current_time = asyncio.get_event_loop().time()
        
        if not user_data["groups"]:
            await context.bot.send_message(
                chat_id=context._chat_id,
                text="No groups to post to. Stopping posting."
            )
            user_data["posting"] = False
            save_user_data(user_id, user_data)
            break
            
        await cleanup_idle_clients()
        
        # Get fresh list of accounts each iteration
        sessions = get_all_sessions(user_id)
        if not sessions:
            await asyncio.sleep(5)
            continue
            
        for group in list(user_data["groups"]):
            try:
                # Check if enough time has passed for this group
                if current_time - last_post_time[group] >= user_data["post_interval"]:
                    # Get next account in rotation
                    account_idx = account_rotation[group] % len(sessions)
                    phone = sessions[account_idx]
                    
                    # Attempt to send message
                    client = await get_client(user_id, phone)
                    if client and await client.is_user_authorized():
                        ok, msg = await send_message_as_account(client, group, user_data["ad_message"])
                        if ok:
                            last_post_time[group] = current_time
                            error_counts[group] = 0
                            # Only rotate on successful send
                            account_rotation[group] += 1
                        else:
                            error_counts[group] += 1
                            
                        await context.bot.send_message(
                            chat_id=context._chat_id,
                            text=f"To {group}: {'✅' if ok else '❌'} {msg} (Account: {phone})"
                        )
                        
                        if error_counts[group] >= max_errors:
                            await context.bot.send_message(
                                chat_id=context._chat_id,
                                text=f"Removing {group} after {max_errors} failed attempts."
                            )
                            user_data["groups"].remove(group)
                            save_user_data(user_id, user_data)
                            
                        # Add delay between messages
                        await asyncio.sleep(5)  # Increased delay between messages
                        
            except Exception as e:
                logger.error(f"Error posting to {group}: {e}")
                
        # Calculate next sleep time
        next_post = min(
            [last_post_time[group] + user_data["post_interval"] - current_time 
             for group in user_data["groups"]],
            default=1
        )
        await asyncio.sleep(max(next_post, 5))  # Minimum 5 second sleep
# ==== BOT HANDLERS ====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    # If user is not in permissions, add them as unapproved
    if str(user_id) not in user_permissions:
        user_permissions[str(user_id)] = {"approved": False, "is_admin": False}
        save_permissions()
        
        # Notify admin
        if str(user_id) != str(ADMIN_ID):
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"New user request:\nID: {user_id}\nUsername: @{update.effective_user.username or 'N/A'}"
            )
    
    if not is_approved(user_id):
        await update.message.reply_text(
            "Your access is pending approval. The admin has been notified.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    
    await update.message.reply_text(
        "Welcome! Use the buttons below:", 
        reply_markup=main_menu_markup
    )
    return MENU
async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == "Admin" and is_admin(user_id):
        await update.message.reply_text("Admin menu:", reply_markup=admin_menu_markup)
        return ADMIN_MENU
    elif text == "Manage Groups":
        keyboard = [["Add Group", "Remove Group"], ["List Groups"], ["Back"]]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("Choose an option:", reply_markup=markup)
        return ADD_GROUP
    elif text == "Manage Message":
        await update.message.reply_text(
            "Send the new ad message (or /cancel to abort):", 
            reply_markup=ReplyKeyboardRemove()
        )
        return MANAGE_MESSAGE
    elif text == "Manage Accounts":
        keyboard = [["Add Account", "Remove Account"], ["List Accounts"], ["Back"]]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text("Choose an option:", reply_markup=markup)
        return ADD_ACCOUNT
    elif text == "Manage Interval":
        user_data = load_user_data(user_id)
        await update.message.reply_text(
            f"Current interval: {user_data['post_interval']} seconds.\n"
            "Send the new interval in seconds (e.g., 60), or /cancel to abort.",
            reply_markup=ReplyKeyboardRemove()
        )
        return MANAGE_INTERVAL
    elif text == "Start Posting":
        user_data = load_user_data(user_id)
        if user_data["posting"]:
            await update.message.reply_text("Already posting ads.", reply_markup=main_menu_markup)
            return MENU
        if not user_data["groups"]:
            await update.message.reply_text("No groups to post to.", reply_markup=main_menu_markup)
            return MENU
        if not get_all_sessions(user_id):
            await update.message.reply_text("No accounts available. Add accounts first.", reply_markup=main_menu_markup)
            return MENU
            
        user_data["posting"] = True
        save_user_data(user_id, user_data)
        chat_id = update.effective_chat.id
        context._chat_id = chat_id
        
        # Cancel any existing posting task for this user
        if f"posting_task_{user_id}" in context.bot_data:
            old_task = context.bot_data[f"posting_task_{user_id}"]
            if not old_task.done():
                old_task.cancel()
                
        # Create new posting task
        posting_task = asyncio.create_task(post_to_groups(user_id, context))
        context.bot_data[f"posting_task_{user_id}"] = posting_task
        
        await update.message.reply_text("Bot started posting ads!", reply_markup=main_menu_markup)
        return MENU
    elif text == "Stop Posting":
        user_data = load_user_data(user_id)
        if user_data["posting"]:
            user_data["posting"] = False
            save_user_data(user_id, user_data)
            
            # Cancel the posting task
            if f"posting_task_{user_id}" in context.bot_data:
                task = context.bot_data[f"posting_task_{user_id}"]
                if not task.done():
                    task.cancel()
                del context.bot_data[f"posting_task_{user_id}"]
            
            await update.message.reply_text("Stopped posting ads.", reply_markup=main_menu_markup)
        else:
            await update.message.reply_text("Bot is not posting.", reply_markup=main_menu_markup)
        return MENU
    else:
        await update.message.reply_text("Please select an option from the menu.", reply_markup=main_menu_markup)
        return MENU

# ==== ADMIN HANDLERS ====
async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("You are not authorized to access this menu.", reply_markup=main_menu_markup)
        return MENU
        
    text = update.message.text
    if text == "List Users":
        users_list = []
        for uid, data in user_permissions.items():
            status = "Admin" if data.get("is_admin", False) else ("Approved" if data.get("approved", False) else "Pending")
            users_list.append(f"ID: {uid} - Status: {status}")
        
        await update.message.reply_text(
            "Users:\n" + "\n".join(users_list) if users_list else "No users yet.",
            reply_markup=admin_menu_markup
        )
        return ADMIN_MENU
    elif text == "Approve Users":
        pending_users = [uid for uid, data in user_permissions.items() if not data.get("approved", False) and not data.get("is_admin", False)]
        if not pending_users:
            await update.message.reply_text("No pending user requests.", reply_markup=admin_menu_markup)
            return ADMIN_MENU
            
        keyboard = [[uid] for uid in pending_users] + [["Back"]]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            "Select user to approve:", 
            reply_markup=markup
        )
        return APPROVE_USER
    elif text == "Revoke Access":
        approved_users = [uid for uid, data in user_permissions.items() if data.get("approved", False) and not data.get("is_admin", False)]
        if not approved_users:
            await update.message.reply_text("No users to revoke access from.", reply_markup=admin_menu_markup)
            return ADMIN_MENU
            
        keyboard = [[uid] for uid in approved_users] + [["Back"]]
        markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(
            "Select user to revoke access:", 
            reply_markup=markup
        )
        return APPROVE_USER
    elif text == "Back":
        await update.message.reply_text("Back to main menu.", reply_markup=main_menu_markup)
        return MENU
    else:
        await update.message.reply_text("Please select an option from the menu.", reply_markup=admin_menu_markup)
        return ADMIN_MENU

async def approve_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("You are not authorized to access this menu.", reply_markup=main_menu_markup)
        return MENU
        
    text = update.message.text
    if text == "Back":
        await update.message.reply_text("Back to admin menu.", reply_markup=admin_menu_markup)
        return ADMIN_MENU
        
    # Check if we're approving or revoking
    action = "approve" if user_permissions.get(text, {}).get("approved", False) is False else "revoke"
    
    if text in user_permissions:
        if action == "approve":
            user_permissions[text]["approved"] = True
            await context.bot.send_message(
                chat_id=int(text),
                text="Your access has been approved by admin! Use /start to begin."
            )
            msg = f"Approved user {text}"
        else:
            user_permissions[text]["approved"] = False
            await context.bot.send_message(
                chat_id=int(text),
                text="Your access has been revoked by admin."
            )
            msg = f"Revoked access for user {text}"
            
        save_permissions()
        await update.message.reply_text(msg, reply_markup=admin_menu_markup)
    else:
        await update.message.reply_text("Invalid user ID.", reply_markup=admin_menu_markup)
    
    return ADMIN_MENU
# Add these new helper functions
async def resolve_group_entity(client, group_identifier):
    """Convert group identifier to Telethon entity"""
    try:
        # Try by username/invite link first
        return await client.get_entity(group_identifier)
    except ValueError:
        # Try by numeric ID if username fails
        try:
            return await client.get_entity(int(group_identifier))
        except:
            return None

async def join_group(client, group_identifier):
    """Attempt to join a group with error handling"""
    try:
        entity = await resolve_group_entity(client, group_identifier)
        if not entity:
            return False, "Invalid group identifier"
            
        # Check if already in group
        participant = await client.get_participants(entity, limit=1)
        if not participant:
            await client.join_chat(entity)
            return True, "Joined group successfully"
        return True, "Already in group"
    except FloodWaitError as e:
        return False, f"Need to wait {e.seconds} seconds"
    except Exception as e:
        return False, str(e)

# Modified group management handler
async def manage_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    user_data = load_user_data(user_id)
    text = update.message.text
    
    if text == "Add Group":
        await update.message.reply_text(
            "Send group usernames/links/IDs (one per line):",
            reply_markup=ReplyKeyboardRemove()
        )
        return ADD_GROUP

# ==== GROUP MANAGEMENT ====
async def manage_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    user_data = load_user_data(user_id)
    text = update.message.text
    
    if text == "Add Group":
        await update.message.reply_text(
            "Send the group username (e.g. @MyGroup), invite link, or chat ID. You can send multiple, separated by newlines.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ADD_GROUP
    elif text == "Remove Group":
        if not user_data["groups"]:
            await update.message.reply_text("No groups to remove.", reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True))
            return ADD_GROUP
        await update.message.reply_text(
            "Send the group username, invite link, or chat ID to remove. You can send multiple, separated by newlines.",
            reply_markup=ReplyKeyboardRemove()
        )
        return REMOVE_GROUP
    elif text == "List Groups":
        if not user_data["groups"]:
            await update.message.reply_text("No groups added.", reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True))
        else:
            await update.message.reply_text(
                "Groups:\n" + "\n".join(user_data["groups"]), 
                reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
            )
        return ADD_GROUP
    elif text == "Back":
        await update.message.reply_text("Back to main menu.", reply_markup=main_menu_markup)
        return MENU
    else:
        entries = [g.strip() for g in text.split("\n") if g.strip()]
        added = []
        for entry in entries:
            if entry not in user_data["groups"]:
                user_data["groups"].append(entry)
                added.append(entry)
        
        if added:
            save_user_data(user_id, user_data)
            await update.message.reply_text(
                f"Added groups:\n" + "\n".join(added), 
                reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
            )
        else:
            await update.message.reply_text(
                "No new groups added.", 
                reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
            )
        return ADD_GROUP

async def remove_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    user_data = load_user_data(user_id)
    text = update.message.text
    
    if text == "Back":
        await update.message.reply_text(
            "Back to group management.", 
            reply_markup=ReplyKeyboardMarkup([["Add Group", "Remove Group"], ["List Groups"], ["Back"]], resize_keyboard=True)
        )
        return ADD_GROUP
        
    entries = [g.strip() for g in text.split("\n") if g.strip()]
    removed = []
    for entry in entries:
        if entry in user_data["groups"]:
            user_data["groups"].remove(entry)
            removed.append(entry)
    
    if removed:
        save_user_data(user_id, user_data)
        await update.message.reply_text(
            f"Removed groups:\n" + "\n".join(removed), 
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
    else:
        await update.message.reply_text(
            "No matching groups found.", 
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
    return REMOVE_GROUP

# ==== MESSAGE MANAGEMENT ====
async def manage_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    user_data = load_user_data(user_id)
    text = update.message.text
    
    if text == "/cancel":
        await update.message.reply_text("Message update canceled.", reply_markup=main_menu_markup)
        return MENU
        
    user_data["ad_message"] = text
    save_user_data(user_id, user_data)
    await update.message.reply_text("Ad message updated.", reply_markup=main_menu_markup)
    return MENU

# ==== ACCOUNT MANAGEMENT ====
async def manage_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == "Add Account":
        await update.message.reply_text(
            "Send the phone number (with country code, e.g. +123456789) to add the Telegram account.",
            reply_markup=ReplyKeyboardRemove()
        )
        return ADD_ACCOUNT_PHONE
    elif text == "Remove Account":
        sessions = get_all_sessions(user_id)
        if not sessions:
            await update.message.reply_text("No accounts to remove.", reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True))
            return ADD_ACCOUNT
        await update.message.reply_text(
            "Send phone number(s) to remove (with country code), one per line.",
            reply_markup=ReplyKeyboardRemove()
        )
        return REMOVE_ACCOUNT
    elif text == "List Accounts":
        sessions = get_all_sessions(user_id)
        if not sessions:
            await update.message.reply_text("No accounts added.", reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True))
        else:
            await update.message.reply_text(
                "Accounts:\n" + "\n".join(sessions), 
                reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
            )
        return ADD_ACCOUNT
    elif text == "Back":
        await update.message.reply_text("Back to main menu.", reply_markup=main_menu_markup)
        return MENU
    else:
        await update.message.reply_text("Please use the menu.", reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True))
        return ADD_ACCOUNT



# ==== ACCOUNT MANAGEMENT ====
async def add_account_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    phone = update.message.text.strip()
    
    try:
        # Check if session already exists
        sessions = get_all_sessions(user_id)
        if phone in sessions:
            client = await get_client(user_id, phone)
            if await client.is_user_authorized():
                await update.message.reply_text(
                    "Account is already authorized.", 
                    reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
                )
                return ADD_ACCOUNT
                
        # Create new session
        client = await get_client(user_id, phone)
        if not client:
            await update.message.reply_text(
                "Failed to connect. Try again or check your internet connection.",
                reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
            )
            return ADD_ACCOUNT
            
        if not await client.is_user_authorized():
            sent = await client.send_code_request(phone)
            
            # Store verification data
            pending_phones[user_id] = {
                "phone": phone,
                "phone_code_hash": sent.phone_code_hash
            }
            
            await update.message.reply_text(
                "A code was sent to your Telegram app. Please send the code here."
            )
            return ADD_ACCOUNT_CODE
        else:
            await update.message.reply_text(
                "Account is already authorized.", 
                reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
            )
            return ADD_ACCOUNT
    except FloodWaitError as e:
        wait_time = e.seconds
        await update.message.reply_text(
            f"Rate limited by Telegram. Need to wait {wait_time} seconds before trying again.",
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
        return ADD_ACCOUNT
    except Exception as e:
        logger.error(f"Error in add_account_phone: {e}")
        await update.message.reply_text(
            f"Failed to send code: {str(e)[:100]}",
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
        return ADD_ACCOUNT

async def add_account_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    code = update.message.text.strip()
    
    # Get stored verification data
    if user_id not in pending_phones:
        await update.message.reply_text(
            "Session expired. Start again with 'Add Account'.",
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
        return ADD_ACCOUNT
        
    verification_data = pending_phones[user_id]
    phone = verification_data["phone"]
    phone_code_hash = verification_data["phone_code_hash"]
    
    # Handle 2FA password if needed
    if context.user_data.get('awaiting_password'):
        password = code
        try:
            client = await get_client(user_id, phone)
            if not client:
                raise Exception("Failed to connect")
                
            await client.sign_in(password=password)
            await update.message.reply_text(
                "Account added and authorized with 2FA!",
                reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
            )
            context.user_data['awaiting_password'] = False
            del pending_phones[user_id]  # Clear after successful auth
            return ADD_ACCOUNT
        except Exception as e:
            await update.message.reply_text(
                f"Failed to sign in with password: {str(e)[:100]}",
                reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
            )
            return ADD_ACCOUNT
    
    # Regular code verification
    try:
        client = await get_client(user_id, phone)
        if not client:
            raise Exception("Failed to connect")
            
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        await update.message.reply_text(
            "Account added and authorized!",
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
        del pending_phones[user_id]  # Clear after successful auth
        return ADD_ACCOUNT
    except SessionPasswordNeededError:
        await update.message.reply_text("Two-factor authentication enabled. Send your password.")
        context.user_data['awaiting_password'] = True
        return ADD_ACCOUNT_CODE
    except Exception as e:
        logger.error(f"Error in add_account_code: {e}")
        await update.message.reply_text(
            f"Failed to authorize: {str(e)[:100]}",
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
        return ADD_ACCOUNT

async def remove_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    text = update.message.text
    
    if text == "Back":
        await update.message.reply_text(
            "Back to account management.", 
            reply_markup=ReplyKeyboardMarkup([["Add Account", "Remove Account"], ["List Accounts"], ["Back"]], resize_keyboard=True)
        )
        return ADD_ACCOUNT
        
    entries = [a.strip() for a in text.split("\n") if a.strip()]
    removed = []
    
    for phone in entries:
        # Disconnect and clean up client if it's in the cache
        cache_key = f"{user_id}_{phone}"
        if cache_key in client_cache:
            try:
                client = client_cache[cache_key]
                if client.is_connected():
                    await client.disconnect()
                del client_cache[cache_key]
                if cache_key in client_last_used:
                    del client_last_used[cache_key]
            except Exception as e:
                logger.error(f"Error disconnecting client for {phone}: {e}")
        
        # Remove session file
        session_file = os.path.join(SESSION_DIR, f"{user_id}_{phone}.session")
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
                removed.append(phone)
            except Exception as e:
                logger.error(f"Error removing session file for {phone}: {e}")
    
    if removed:
        await update.message.reply_text(
            f"Removed accounts:\n" + "\n".join(removed),
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
    else:
        await update.message.reply_text(
            "No matching accounts found.",
            reply_markup=ReplyKeyboardMarkup([["Back"]], resize_keyboard=True)
        )
    return REMOVE_ACCOUNT

# ==== INTERVAL MANAGEMENT ====
async def manage_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_access(update, context):
        return ConversationHandler.END
        
    user_id = update.effective_user.id
    user_data = load_user_data(user_id)
    text = update.message.text
    
    if text == "/cancel":
        await update.message.reply_text("Interval update canceled.", reply_markup=main_menu_markup)
        return MENU
        
    try:
        interval = int(text)
        if interval < 5:
            await update.message.reply_text("Interval too short. Set at least 5 seconds:")
            return MANAGE_INTERVAL
            
        user_data["post_interval"] = interval
        save_user_data(user_id, user_data)
        await update.message.reply_text(
            f"Interval updated to {interval} seconds.", 
            reply_markup=main_menu_markup
        )
        return MENU
    except ValueError:
        await update.message.reply_text("Please send a valid number (seconds):")
        return MANAGE_INTERVAL

async def cleanup_handler(application):
    """Cleanup when application is shutting down"""
    # Disconnect all clients
    for key, client in list(client_cache.items()):
        try:
            if client.is_connected():
                await client.disconnect()
            logger.info(f"Disconnected client for {key}")
        except Exception as e:
            logger.error(f"Error disconnecting client for {key}: {e}")

# ==== MAIN ====
def main():
    # Load permissions at startup
    load_permissions()
    
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    # Set up conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MENU: [MessageHandler(filters.TEXT & (~filters.COMMAND), menu)],
            ADMIN_MENU: [MessageHandler(filters.TEXT & (~filters.COMMAND), admin_menu)],
            APPROVE_USER: [MessageHandler(filters.TEXT & (~filters.COMMAND), approve_user)],
            ADD_GROUP: [
                MessageHandler(filters.Regex("^(Add Group|Remove Group|List Groups|Back)$"), manage_groups),
                MessageHandler(filters.TEXT & (~filters.COMMAND), manage_groups)
            ],
            REMOVE_GROUP: [
                MessageHandler(filters.Regex("^(Back)$"), remove_groups),
                MessageHandler(filters.TEXT & (~filters.COMMAND), remove_groups)
            ],
            MANAGE_MESSAGE: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), manage_message)
            ],
            ADD_ACCOUNT: [
                MessageHandler(filters.Regex("^(Add Account|Remove Account|List Accounts|Back)$"), manage_accounts),
                MessageHandler(filters.TEXT & (~filters.COMMAND), manage_accounts)
            ],
            ADD_ACCOUNT_PHONE: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), add_account_phone)
            ],
            ADD_ACCOUNT_CODE: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), add_account_code)
            ],
            REMOVE_ACCOUNT: [
                MessageHandler(filters.Regex("^(Back)$"), remove_accounts),
                MessageHandler(filters.TEXT & (~filters.COMMAND), remove_accounts)
            ],
            MANAGE_INTERVAL: [
                MessageHandler(filters.TEXT & (~filters.COMMAND), manage_interval)
            ]
        },
        fallbacks=[CommandHandler("start", start)]
    )

    app.add_handler(conv_handler)
    
    # Set up shutdown handler
    app.post_shutdown = cleanup_handler
    
    # Set larger timeout for connections to Telegram API
    app.run_polling(timeout=30)

if __name__ == "__main__":
    main()