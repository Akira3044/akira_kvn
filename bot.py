import asyncio
import logging
import json
import os
import secrets
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, 
    InlineKeyboardMarkup, InlineKeyboardButton
)
from dotenv import load_dotenv
import httpx

# --- КОНФИГУРАЦИЯ ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMINS = [int(id_str) for id_str in os.getenv("ADMINS", "").split(",") if id_str]
CHAT_ID_1 = int(os.getenv("CHAT_ID_1", "0"))
CHAT_ID_2 = int(os.getenv("CHAT_ID_2", "0"))
TONAPI_KEY = os.getenv("TONAPI_KEY")
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS")
DATA_FILE = "data.json"

# Лимиты ключей за нахождение в чатах (настраиваемо)
KEYS_PER_CHAT = 3 

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# --- РАБОТА С ФАЙЛОМ (БАЗА ДАННЫХ) ---

def load_data() -> Dict[str, Any]:
    if not os.path.exists(DATA_FILE):
        return {"users": {}, "pending": {}}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {"users": {}, "pending": {}}

def save_data(data: Dict[str, Any]):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def ensure_user(user_id: str, username: str, data: Dict):
    if user_id not in data["users"]:
        data["users"][user_id] = {
            "username": username,
            "manual_limit": None, # Если None, считается автоматически
            "keys_used": 0,
            "keys": []
        }
    else:
        # Обновляем юзернейм если изменился
        data["users"][user_id]["username"] = username

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

async def calculate_limit(user_id: int, user_data: Dict) -> int:
    """
    Считает лимит. Приоритет: 
    1. Ручной лимит админа (если установлен).
    2. Сумма бонусов за чаты.
    """
    # Если админ установил жесткий лимит
    if user_data.get("manual_limit") is not None:
        return user_data["manual_limit"]

    limit = 0
    # Проверка чата 1
    if CHAT_ID_1:
        try:
            member = await bot.get_chat_member(chat_id=CHAT_ID_1, user_id=user_id)
            if member.status in ['member', 'administrator', 'creator']:
                limit += KEYS_PER_CHAT
        except Exception as e:
            logger.warning(f"Ошибка проверки чата 1 для {user_id}: {e}")

    # Проверка чата 2
    if CHAT_ID_2:
        try:
            member = await bot.get_chat_member(chat_id=CHAT_ID_2, user_id=user_id)
            if member.status in ['member', 'administrator', 'creator']:
                limit += KEYS_PER_CHAT
        except Exception as e:
            logger.warning(f"Ошибка проверки чата 2 для {user_id}: {e}")
    
    # Базовый лимит, если не в чатах (например, 1 пробный)
    if limit == 0:
        limit = 1 
        
    return limit

def generate_key_string() -> str:
    """Генерирует случайный ключ"""
    return f"KVN-{secrets.token_hex(4).upper()}"

# --- МАШИНА СОСТОЯНИЙ (FSM) ДЛЯ АДМИНА ---
class AdminState(StatesGroup):
    waiting_for_user_input = State()

# --- КЛАВИАТУРЫ ---

def get_main_menu(is_admin: bool = False):
    buttons = [
        [KeyboardButton(text="🛒 Купить КВН"), KeyboardButton(text="🎁 Получить ключ")],
        [KeyboardButton(text="📖 Мои КВН"), KeyboardButton(text="ℹ️ Помощь")]
    ]
    if is_admin:
        buttons.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def get_help_keyboard():
    buttons = [
        [InlineKeyboardButton(text="📄 Инструкция", callback_data="help_instr"),
         InlineKeyboardButton(text="🌐 Локации серверов", callback_data="help_loc")],
        [InlineKeyboardButton(text="⚡ Обход отключений", callback_data="help_bypass"),
         InlineKeyboardButton(text="🔌 Подключение v2Ray", callback_data="help_v2ray")],
        [InlineKeyboardButton(text="🛠 Решение проблем", callback_data="help_trouble"),
         InlineKeyboardButton(text="💡 Полезные фичи", callback_data="help_features")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="help_back")] # Просто скрывает сообщение
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_limits_keyboard(target_user_id: str):
    # Кнопки для быстрой установки лимита
    limits = [1, 8, 16, 100]
    buttons = []
    row = []
    for lim in limits:
        row.append(InlineKeyboardButton(text=str(lim), callback_data=f"set_lim_{target_user_id}_{lim}"))
    buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- ХЕНДЛЕРЫ: СТАРТ И МЕНЮ ---

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    user_id = str(message.from_user.id)
    data = load_data()
    ensure_user(user_id, message.from_user.username, data)
    save_data(data)
    
    is_admin = message.from_user.id in ADMINS
    await message.answer(
        "👋 Привет! Это бот выдачи ключей КВН.\n"
        "Используйте меню для управления.",
        reply_markup=get_main_menu(is_admin)
    )

@dp.message(F.text == "⬅ В меню")
async def back_to_menu(message: types.Message):
    await start_cmd(message)

# --- ХЕНДЛЕРЫ: ВЫДАЧА КЛЮЧЕЙ ---

@dp.message(F.text == "🎁 Получить ключ")
async def get_key_handler(message: types.Message):
    user_id_str = str(message.from_user.id)
    data = load_data()
    ensure_user(user_id_str, message.from_user.username, data)
    
    # 1. Считаем текущий лимит (асинхронно, т.к. запросы к API телеграм)
    limit = await calculate_limit(message.from_user.id, data["users"][user_id_str])
    
    user_record = data["users"][user_id_str]
    used = len(user_record["keys"]) # Фактическое количество активных ключей
    
    # 2. Проверяем лимит
    if used >= limit:
        await message.answer(
            f"⛔ <b>Лимит исчерпан!</b>\n"
            f"Ваш лимит: {limit} шт.\n"
            f"Получено: {used}/{limit}.\n\n"
            f"Чтобы увеличить лимит, вступите в наши чаты или купите подписку.",
            parse_mode="HTML"
        )
        return

    # 3. Генерируем ключ
    new_key_value = generate_key_string()
    expiration_date = datetime.now() + timedelta(days=30)
    
    key_obj = {
        "id": secrets.token_hex(3).upper(),
        "key": new_key_value,
        "valid_until": expiration_date.timestamp(),
        "created_at": datetime.now().timestamp()
    }
    
    # 4. Сохраняем
    user_record["keys"].append(key_obj)
    user_record["keys_used"] = len(user_record["keys"]) # Обновляем счетчик
    save_data(data)
    
    # 5. Отправляем
    await message.answer(
        f"✅ <b>Ключ успешно выдан!</b>\n\n"
        f"🔑 <code>{new_key_value}</code>\n"
        f"📅 Годен до: {expiration_date.strftime('%d.%m.%Y')}\n\n"
        f"📊 Прогресс: получен ключ {len(user_record['keys'])}/{limit}",
        parse_mode="HTML"
    )

@dp.message(F.text == "📖 Мои КВН")
async def my_keys_handler(message: types.Message):
    user_id_str = str(message.from_user.id)
    data = load_data()
    ensure_user(user_id_str, message.from_user.username, data)
    
    keys = data["users"][user_id_str]["keys"]
    
    if not keys:
        await message.answer("📂 У вас пока нет активных ключей.")
        return
        
    response = "<b>📂 Ваши ключи:</b>\n\n"
    current_time = datetime.now().timestamp()
    
    # Фильтрация истекших ключей (опционально можно удалять их)
    active_keys = []
    
    for k in keys:
        if k["valid_until"] > current_time:
            date_str = datetime.fromtimestamp(k["valid_until"]).strftime('%d.%m.%Y')
            response += f"🔑 <code>{k['key']}</code> (до {date_str})\n"
            active_keys.append(k)
        else:
            # Ключ истек
            response += f"❌ <s>{k['key']}</s> (Истёк)\n"
    
    # Обновляем JSON, убирая старые ключи (если нужно, раскомментируй)
    # data["users"][user_id_str]["keys"] = active_keys
    # save_data(data)
            
    await message.answer(response, parse_mode="HTML")

# --- ХЕНДЛЕРЫ: ПОМОЩЬ ---

@dp.message(F.text == "ℹ️ Помощь")
async def help_menu_handler(message: types.Message):
    await message.answer("📚 Выберите раздел помощи:", reply_markup=get_help_keyboard())

@dp.callback_query(F.data.startswith("help_"))
async def help_callback_handler(callback: types.CallbackQuery):
    action = callback.data.split("_")[1]
    
    texts = {
        "instr": "📄 <b>Инструкция:</b>\n1. Скачайте клиент V2Ray.\n2. Скопируйте ключ.\n3. Импортируйте из буфера обмена.\n4. Нажмите Connect.",
        "loc": "🌐 <b>Локации:</b>\n- Германия 🇩🇪\n- Нидерланды 🇳🇱\n- США 🇺🇸",
        "bypass": "⚡ <b>Обход блокировок:</b>\nМы используем протоколы VLESS + Reality для максимальной скрытности.",
        "v2ray": "🔌 <b>Подключение:</b>\nСкачайте приложение:\n- Android: v2rayNG\n- iOS: FoXray / Shadowrocket\n- PC: v2rayN",
        "trouble": "🛠 <b>Решение проблем:</b>\nЕсли не подключается, проверьте синхронизацию времени на устройстве.",
        "features": "💡 <b>Фичи:</b>\n- Высокая скорость\n- Безлимитный трафик\n- Поддержка UDP",
    }
    
    if action == "back":
        await callback.message.delete()
        return

    text = texts.get(action, "Информация отсутствует.")
    # Редактируем сообщение, оставляя клавиатуру
    await callback.message.edit_text(text, reply_markup=get_help_keyboard(), parse_mode="HTML")
    await callback.answer()

# --- ХЕНДЛЕРЫ: АДМИН ПАНЕЛЬ ---

@dp.message(F.text == "⚙️ Админ-панель")
async def admin_panel_handler(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        return
    
    await message.answer(
        "🔧 <b>Админ-панель</b>\n"
        "Введите ID пользователя или Username (начинается с @), чтобы изменить его лимит.",
        parse_mode="HTML"
    )
    await state.set_state(AdminState.waiting_for_user_input)

@dp.message(AdminState.waiting_for_user_input)
async def admin_user_search(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        return

    query = message.text.strip()
    data = load_data()
    target_id = None
    
    # Поиск по ID или Username
    if query.isdigit():
        target_id = query
    elif query.startswith("@"):
        uname = query[1:]
        for uid, udata in data["users"].items():
            if udata.get("username") == uname:
                target_id = uid
                break
    
    if not target_id or target_id not in data["users"]:
        await message.answer("❌ Пользователь не найден в базе (он должен хоть раз запустить бота). Попробуйте снова.")
        return

    # Сохраняем ID найденного юзера в FSM storage, чтобы не потерять
    await state.update_data(target_user_id=target_id)
    
    user_info = data["users"][target_id]
    current_lim = user_info.get("manual_limit", "Авто (по чатам)")
    
    await message.answer(
        f"👤 Пользователь найден: <code>{target_id}</code> (@{user_info.get('username')})\n"
        f"Текущий лимит: {current_lim}\n\n"
        f"Выберите новый лимит:",
        reply_markup=get_admin_limits_keyboard(target_id),
        parse_mode="HTML"
    )
    await state.clear() # Сбрасываем состояние, так как дальше работаем через инлайн кнопки

@dp.callback_query(F.data.startswith("set_lim_"))
async def set_limit_callback(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMINS:
        await callback.answer("Нет прав.", show_alert=True)
        return

    _, target_id, limit_val = callback.data.split("_")
    new_limit = int(limit_val)
    
    data = load_data()
    if target_id in data["users"]:
        data["users"][target_id]["manual_limit"] = new_limit
        save_data(data)
        
        await callback.message.edit_text(
            f"✅ Лимит для пользователя <code>{target_id}</code> установлен на <b>{new_limit}</b>.",
            parse_mode="HTML"
        )
        
        # Оповещение пользователю (опционально)
        try:
            await bot.send_message(target_id, f"🎉 Ваш лимит ключей обновлен! Теперь доступно: {new_limit}")
        except:
            pass
            
    else:
        await callback.answer("Ошибка: пользователь не найден.", show_alert=True)

# --- TON ПЛАТЕЖИ (ЗАГЛУШКА) ---

@dp.message(F.text == "🛒 Купить КВН")
async def buy_key_handler(message: types.Message):
    # Генерация invoice (заглушка)
    amount_ton = 0.5
    comment = f"pay_{message.from_user.id}_{int(time.time())}"
    
    # Здесь должен быть реальный deeplink
    link = f"ton://transfer/{WALLET_ADDRESS}?amount={int(amount_ton*1e9)}&text={comment}"
    
    # Сохраняем в pending
    data = load_data()
    data["pending"][comment] = {
        "user_id": str(message.from_user.id),
        "amount": amount_ton,
        "created_at": time.time(),
        "status": "waiting"
    }
    save_data(data)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Оплатить", url=link)],
        [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_pay_{comment}")]
    ])
    
    await message.answer(
        f"💎 <b>Покупка дополнительного слота</b>\n"
        f"Цена: {amount_ton} TON\n\n"
        f"Нажмите кнопку ниже для оплаты.",
        reply_markup=kb,
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("check_pay_"))
async def check_payment_manual(callback: types.CallbackQuery):
    comment = callback.data.replace("check_pay_", "")
    # В реальном боте здесь запрос к TON API
    # Сделаем эмуляцию для примера:
    
    is_paid = False # Поставь True для теста, чтобы проверить выдачу
    
    if is_paid:
        await finalize_payment(comment, callback.message)
    else:
        await callback.answer("❌ Оплата еще не поступила. Попробуйте через минуту.", show_alert=True)

async def finalize_payment(comment_id: str, message: types.Message = None):
    data = load_data()
    if comment_id in data["pending"]:
        info = data["pending"][comment_id]
        user_id = info["user_id"]
        
        # Выдача награды: Увеличиваем лимит на +1
        if user_id in data["users"]:
            current_manual = data["users"][user_id].get("manual_limit")
            if current_manual is None:
                current_manual = await calculate_limit(int(user_id), data["users"][user_id])
            
            data["users"][user_id]["manual_limit"] = current_manual + 1
            del data["pending"][comment_id]
            save_data(data)
            
            msg = f"✅ Оплата прошла! Ваш лимит увеличен до {data['users'][user_id]['manual_limit']}."
            if message:
                await message.edit_text(msg)
            else:
                await bot.send_message(user_id, msg)

# --- ФОНОВЫЕ ПРОЦЕССЫ ---

async def background_worker():
    """Фоновая задача для проверки платежей (long polling или cron)"""
    while True:
        try:
            # Здесь логика проверки TON API для всех записей в data['pending']
            # await check_all_pending_transactions()
            await asyncio.sleep(60) # Проверка раз в минуту
        except Exception as e:
            logger.error(f"Background worker error: {e}")
            await asyncio.sleep(60)

# --- ЗАПУСК ---

async def main():
    # Запуск фоновой задачи
    asyncio.create_task(background_worker())
    
    try:
        # Удаляем вебхук и запускаем поллинг
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error starting bot: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped!")
