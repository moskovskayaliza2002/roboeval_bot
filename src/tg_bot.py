#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import random
from pathlib import Path
from typing import Optional, Dict, Any

import aiosqlite
import nest_asyncio
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

nest_asyncio.apply()

# ---------------------------------------------------------------------------
#                          ПУТИ И НАСТРОЙКА ЛОГИРОВАНИЯ
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_PATH = BASE_DIR / "token" / "config.txt"
DB_PATH = BASE_DIR / "data" / "ratings.db"
LOG_DIR = BASE_DIR / "logs"

for p in (TOKEN_PATH.parent, DB_PATH.parent, LOG_DIR):
    p.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("experiment_bot")

try:
    with open(TOKEN_PATH, "r", encoding="utf-8") as f:
        BOT_TOKEN = f.read().strip()
except FileNotFoundError:
    logger.critical(f"❌ Файл с токеном не найден: {TOKEN_PATH}")
    raise
except Exception as e:
    logger.critical(f"❌ Ошибка чтения токена: {e}")
    raise

# ---------------------------------------------------------------------------
#                           КОНСТАНТЫ ЭКСПЕРИМЕНТА
# ---------------------------------------------------------------------------

# Сценарии
SCENARIOS = ["Пицца", "Наперстки", "Детали", "Шахматы"]

# Два типа условий: «без» и «с»
VIDEO_CONDITIONS = ["без", "с"]

# file_id можно получить отправив видео боту, он пришлёт ID в ответ (handle_video).
VIDEO_FILES = {
    "без": {
        "Пицца":    "BAACAgIAAxkBAAMDaR3tILxQxDadmZUPnNfLMYKDklYAAuKGAAK3mfBIUqHZvFT5t4k2BA",
        "Наперстки": "BAACAgIAAxkBAAMHaR3tTeFefOgTqLVzPGWVfq0HqMYAAuWGAAK3mfBInbE1O9rwros2BA",
        "Детали":   "BAACAgIAAxkBAAMLaR3tr3EhNtwKcDD4G-MQ6CSg12wAAumGAAK3mfBIhZdSRIOeY9Y2BA",
        "Шахматы":  "BAACAgIAAxkBAAMPaR3t020V9VOwPFXHjjcMSLTB1C0AAuuGAAK3mfBIaIB8oId19Wc2BA",
    },
    "с": {
        "Пицца":    "BAACAgIAAxkBAAMFaR3tP22-guoJN43uoEp3wNG8O7IAAuOGAAK3mfBIzJd2fp1Quv42BA",
        "Наперстки": "BAACAgIAAxkBAAMJaR3tl8kAAYky84H0z-zVin07co-0AALohgACt5nwSPu4xsh4TUauNgQ",
        "Детали":   "BAACAgIAAxkBAAMNaR3twWb_7p9gokCji027ULfVxrsAAuqGAAK3mfBIbLKHzk9zTIU2BA",
        "Шахматы":  "BAACAgIAAxkBAAMRaR3t7KdwvysGkOElyJnc_lgQitQAAu2GAAK3mfBImFgME8KSjbs2BA",
    },
}

# Формулировки пар утверждений для шкалы 1–10
SCENARIO_QUESTIONS = {
    "Пицца": (
        "Ф-2 выбирает подходящие ингредиенты для пиццы",
        "Ф-2 получает команду от компьютера указать на ингредиенты слева",
    ),
    "Наперстки": (
        "Ф-2 пытается обыграть испытуемую в напёрстки",
        "Ф-2 получает команду от компьютера указать на средний стаканчик",
    ),
    "Детали": (
        "Ф-2 оценивает, какая деталь подходит для хвоста",
        "Ф-2 получает команду от компьютера указать на деталь с определённым QR-кодом",
    ),
    "Шахматы": (
        "Ф-2 обдумывает свой следующий ход",
        "Ф-2 получает команду от компьютера указать на свободную клетку",
    ),
}

# Ссылка на Google-форму
GOOGLE_FORM_URL = "https://example.com/google-form"  # ЗАМЕНИТЕ НА РЕАЛЬНУЮ ССЫЛКУ


# ---------------------------------------------------------------------------
#                            СОЗДАНИЕ БД
# ---------------------------------------------------------------------------

async def create_schema_and_fill():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON;")
        await db.execute("PRAGMA journal_mode = WAL;")

        # Пользователи Telegram
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                tg_username TEXT,
                first_name  TEXT
            );
            """
        )

        # Участники эксперимента (один проход эксперимента на одного user_id)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS participants (
                user_id           INTEGER PRIMARY KEY,
                tg_username       TEXT,
                first_name        TEXT,
                participant_name  TEXT,   -- имя или псевдоним, который нужно запомнить
                gender            TEXT,
                age               INTEGER,
                condition         TEXT,   -- 'без' или 'с'
                current_video_idx INTEGER NOT NULL DEFAULT 0,
                total_videos      INTEGER NOT NULL DEFAULT 4,
                completed         INTEGER NOT NULL DEFAULT 0, -- 0/1
                created_at        TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            """
        )

        # Порядок видео для каждого пользователя
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS video_sequence (
                user_id   INTEGER NOT NULL,
                position  INTEGER NOT NULL,
                condition TEXT NOT NULL,
                scenario  TEXT NOT NULL,
                file_id   TEXT NOT NULL,
                PRIMARY KEY (user_id, position),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            """
        )

        # Ответы по каждому видео
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS answers (
                user_id         INTEGER NOT NULL,
                position        INTEGER NOT NULL,
                scenario        TEXT NOT NULL,
                file_id         TEXT NOT NULL,
                description     TEXT,      -- описание действий робота
                adv_behavior    TEXT,      -- "Робот ведёт себя ____"
                adv_choice      TEXT,      -- "Робот делает выбор ____"
                scenario_rating INTEGER,   -- 1..10
                PRIMARY KEY (user_id, position),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            """
        )

        await db.commit()


# ---------------------------------------------------------------------------
#                        УТИЛИТЫ РАБОТЫ С БАЗОЙ ДАННЫХ
# ---------------------------------------------------------------------------


async def ensure_user(user_id: int, username: str, first_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users(user_id, tg_username, first_name)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                tg_username = excluded.tg_username,
                first_name  = excluded.first_name
            """,
            (user_id, username or "", first_name or ""),
        )
        await db.commit()


async def get_participant(user_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT user_id, tg_username, first_name, participant_name,
                   gender, age, condition, current_video_idx, total_videos, completed
            FROM participants
            WHERE user_id = ?
            """,
            (user_id,),
        )
        row = await cur.fetchone()

    if not row:
        return None

    return {
        "user_id": row[0],
        "tg_username": row[1],
        "first_name": row[2],
        "participant_name": row[3],
        "gender": row[4],
        "age": row[5],
        "condition": row[6],
        "current_video_idx": row[7],
        "total_videos": row[8],
        "completed": row[9],
    }


async def create_participant(
    user_id: int,
    tg_username: Optional[str],
    first_name: Optional[str],
    participant_name: Optional[str] = None,
    gender: Optional[str] = None,
    age: Optional[int] = None,
    condition: Optional[str] = None,
    total_videos: Optional[int] = None,
):
    """
    Создаёт участника, если его ещё нет, и аккуратно обновляет только те поля,
    которые явно переданы (не None). Так мы не затираем уже записанные данные.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO participants (user_id, tg_username, first_name)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                tg_username = excluded.tg_username,
                first_name  = excluded.first_name
            """,
            (user_id, tg_username or "", first_name or ""),
        )

        if participant_name is not None:
            await db.execute(
                "UPDATE participants SET participant_name = ? WHERE user_id = ?",
                (participant_name, user_id),
            )

        if gender is not None:
            await db.execute(
                "UPDATE participants SET gender = ? WHERE user_id = ?",
                (gender, user_id),
            )

        if age is not None:
            await db.execute(
                "UPDATE participants SET age = ? WHERE user_id = ?",
                (age, user_id),
            )

        if condition is not None:
            await db.execute(
                "UPDATE participants SET condition = ? WHERE user_id = ?",
                (condition, user_id),
            )

        if total_videos is not None:
            await db.execute(
                "UPDATE participants SET total_videos = ? WHERE user_id = ?",
                (total_videos, user_id),
            )

        await db.commit()

async def update_participant_progress(
    user_id: int,
    current_video_idx: Optional[int] = None,
    completed: Optional[bool] = None,
):
    if current_video_idx is None and completed is None:
        return

    fields = []
    params = []

    if current_video_idx is not None:
        fields.append("current_video_idx = ?")
        params.append(current_video_idx)

    if completed is not None:
        fields.append("completed = ?")
        params.append(1 if completed else 0)

    params.append(user_id)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE participants SET {', '.join(fields)} WHERE user_id = ?",
            params,
        )
        await db.commit()


async def create_video_sequence_for_participant(user_id: int, condition: str):
    """
    Формирует порядок из 4 видео (по одному на каждый сценарий) в случайном порядке.
    """
    if condition not in VIDEO_FILES:
        raise ValueError(f"Неизвестное условие: {condition}")

    videos = []
    for scenario in SCENARIOS:
        try:
            file_id = VIDEO_FILES[condition][scenario]
        except KeyError:
            raise ValueError(
                f"Для условия '{condition}' и сценария '{scenario}' не задан file_id в VIDEO_FILES."
            )
        videos.append((scenario, file_id))

    random.shuffle(videos)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM video_sequence WHERE user_id = ?",
            (user_id,),
        )

        for pos, (scenario, file_id) in enumerate(videos):
            await db.execute(
                """
                INSERT INTO video_sequence(user_id, position, condition, scenario, file_id)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, pos, condition, scenario, file_id),
            )

        await db.commit()


async def get_video_by_position(user_id: int, position: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT condition, scenario, file_id
            FROM video_sequence
            WHERE user_id = ? AND position = ?
            """,
            (user_id, position),
        )
        row = await cur.fetchone()

    if not row:
        return None

    return {"condition": row[0], "scenario": row[1], "file_id": row[2]}


async def get_answer(user_id: int, position: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT scenario, file_id, description, adv_behavior, adv_choice, scenario_rating
            FROM answers
            WHERE user_id = ? AND position = ?
            """,
            (user_id, position),
        )
        row = await cur.fetchone()

    if not row:
        return None

    return {
        "scenario": row[0],
        "file_id": row[1],
        "description": row[2],
        "adv_behavior": row[3],
        "adv_choice": row[4],
        "scenario_rating": row[5],
    }


async def upsert_answer_field(
    user_id: int,
    position: int,
    scenario: str,
    file_id: str,
    field_name: str,
    value: Any,
):
    if field_name not in ("description", "adv_behavior", "adv_choice", "scenario_rating"):
        raise ValueError(f"Недопустимое поле answers: {field_name}")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO answers(user_id, position, scenario, file_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, position) DO UPDATE SET
                scenario = excluded.scenario,
                file_id  = excluded.file_id
            """,
            (user_id, position, scenario, file_id),
        )

        await db.execute(
            f"UPDATE answers SET {field_name} = ? WHERE user_id = ? AND position = ?",
            (value, user_id, position),
        )

        await db.commit()


# ---------------------------------------------------------------------------
#                        ЛОГИКА ПРОГРЕССА ПО ЭКСПЕРИМЕНТУ
# ---------------------------------------------------------------------------

async def determine_next_stage(user_id: int) -> str:
    """
    Возвращает, какой следующий шаг нужен пользователю:
    - 'finished'                — все видео пройдены
    - 'expect_description'      — нужно описание действий робота
    - 'expect_adv_behavior'     — нужно наречие поведения
    - 'expect_adv_choice'       — нужно наречие выбора
    - 'expect_rating'           — нужна оценка 1–10
    """
    participant = await get_participant(user_id)
    if not participant:
        return "no_participant"

    if participant["completed"]:
        return "finished"


    idx = participant["current_video_idx"]
    total = participant["total_videos"]

    if idx >= total:
        return "finished"

    ans = await get_answer(user_id, idx)
    if not ans or ans["description"] is None:
        return "expect_description"
    if ans["adv_behavior"] is None:
        return "expect_adv_behavior"
    if ans["adv_choice"] is None:
        return "expect_adv_choice"
    if ans["scenario_rating"] is None:
        return "expect_rating"

    return "expect_description"


async def continue_experiment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    participant = await get_participant(user_id)

    if not participant:
        await update.effective_chat.send_message(
            "Не удалось найти вашу сессию. Нажмите /start, чтобы начать сначала."
        )
        context.user_data.clear()
        return

    if participant["completed"]:
        await send_final_message(update, participant)
        context.user_data.clear()
        return

    idx = participant["current_video_idx"]
    total = participant["total_videos"]

    video_info = await get_video_by_position(user_id, idx)
    if not video_info:
        await update.effective_chat.send_message(
            "Ошибка: не найдена информация о текущем видео. "
            "Пожалуйста, напишите организатору или попробуйте позже."
        )
        return

    stage = await determine_next_stage(user_id)
    context.user_data["stage"] = stage

    scenario = video_info["scenario"]
    file_id = video_info["file_id"]

    if stage == "expect_description":
        caption = (
            f"🎥 Видео {idx + 1} из {total}.\n\n"
            "1️⃣/4️⃣ Пожалуйста, посмотрите это видео со звуком."
        )
        await context.bot.send_video(
            chat_id=update.effective_chat.id,
            video=file_id,
            caption=caption,
        )
        await update.effective_chat.send_message(
            "2️⃣/4️⃣ Опишите, что делает робот на этом видео."
        )
    elif stage == "expect_adv_behavior":
        await update.effective_chat.send_message(
            "3️⃣/4️⃣ Вставьте наречие вместо пробела.\n"
            "Робот ведёт себя ____ (Как?)"
        )
    elif stage == "expect_adv_choice":
        await update.effective_chat.send_message(
            "4️⃣/4️⃣ Вставьте наречие вместо пробела.\n"
            "Робот делает выбор ____ (Как?)"
        )
    elif stage == "expect_rating":
        await send_scenario_rating_question(update, context, scenario)
    elif stage == "finished":
        await send_final_message(update, participant)
        context.user_data.clear()
    else:
        await update.effective_chat.send_message(
            "Произошла ошибка с восстановлением шага. Нажмите /start, чтобы начать заново."
        )
        context.user_data.clear()


async def send_scenario_rating_question(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    scenario: str,
):
    left, right = SCENARIO_QUESTIONS[scenario]
    text = (
        f"🧩 *К какому утверждению вы больше склоняетесь?*\n\n"
        f"1️⃣ {left}\n"
        f"2️⃣ {right}\n\n"
        "Пожалуйста, выберите число от 1 до 10, показывающее, "
        "какое из двух утверждений, на ваш взгляд, лучше всего описывает происходящее на видео: "
        "чем ближе число к 1 — тем больше подходит *первое утверждение*, "
        "чем ближе к 10 — тем больше подходит *второе утверждение*."
    )

    keyboard = []
    row = []
    for i in range(1, 11):
        row.append(InlineKeyboardButton(str(i), callback_data=f"likert_{i}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    await update.effective_chat.send_message(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def send_final_message(update: Update, participant: Dict[str, Any]):
    name = participant.get("participant_name") or "ваше имя/псевдоним"
    text = (
        "Спасибо! Все ваши ответы сохранены.\n\n"
        "Теперь, пожалуйста, перейдите по ссылке для прохождения подробного опроса "
        "о ваших психологических характеристиках, отношении к роботам, ценностях и т.д.\n\n"
        f"👉 {GOOGLE_FORM_URL}\n\n"
        f"Пожалуйста, укажите в Google-форме то же имя или псевдоним, "
        f"который вы написали в начале диалога: «{name}»."
    )
    await update.effective_chat.send_message(text)


# ---------------------------------------------------------------------------
#                               HANDLERS
# ---------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /start:
    1) Если участник уже завершил эксперимент — сообщаем об этом.
    2) Если участник в процессе — восстанавливаем прогресс.
    3) Иначе — объясняем инструкцию и задаём общие вопросы (имя, пол, возраст).
    """
    user = update.effective_user
    user_id = user.id
    await ensure_user(user_id, user.username or "", user.first_name or "")

    participant = await get_participant(user_id)

    if participant and participant["completed"]:
        await update.message.reply_text(
            "🎉 Спасибо за участие! Ваши ответы уже сохранены. "
            "Если хотите проверить, можно ли заполнить Google-форму — ссылка ниже:"
        )
        await send_final_message(update, participant)
        return

    if participant and not participant["completed"]:
        if not participant["participant_name"]:
            context.user_data["stage"] = "ask_name"
            await update.message.reply_text(
                "1️⃣/3️⃣ — Как вас можно назвать в этом исследовании? "
                "Напишите имя или псевдоним (запомните его — потом укажете в Google-форме)."
            )
            return

        if not participant["gender"]:
            context.user_data["stage"] = "ask_gender"
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("👩 Женский", callback_data="gender_female"),
                InlineKeyboardButton("👨 Мужской", callback_data="gender_male")]
            ])
            await update.message.reply_text(
                "2️⃣/3️⃣ — Укажите ваш пол:",
                reply_markup=keyboard
            )
            return

        if participant["age"] is None:
            context.user_data["stage"] = "ask_age"
            await update.message.reply_text(
                "3️⃣/3️⃣ — Укажите, пожалуйста, ваш возраст (числом):"
            )
            return

        await update.message.reply_text(
            "Мы продолжаем ваш эксперимент с того места, где вы остановились."
        )
        await continue_experiment(update, context)
        return

    if not participant:
        instruction = (
            "Спасибо, что согласились принять участие в исследовании "
            "Лаборатории нейрокогнитивных технологий и робототехники (Курчатовский институт).\n\n"
            "Вам нужно будет посмотреть 4 коротких видео и ответить на несколько вопросов по каждому из них — "
            "это займёт примерно 5–10 минут.\n"
            "В конце этого диалога появится ссылка на Google-форму, где нужно будет заполнить более подробный "
            "опрос о ваших психологических характеристиках, отношении к роботам, ценностях и т.д. — "
            "этот опрос займёт примерно 20–25 минут.\n\n"
            "Пожалуйста, укажите одинаковое имя или псевдоним в обеих анкетах — это необходимо для последующего "
            "объединения данных. Участие добровольное, все данные обрабатываются анонимно.\n\n"
            "Рекомендации: для просмотра видео выберите тихое место либо используйте наушники.\n\n"
            "📋 Сначала, ответьте, пожалуйста на *3 личных вопроса*.\n\n"
            "1️⃣/3️⃣ — Как вас можно назвать в этом исследовании? "
            "Напишите имя или псевдоним (запомните его — потом укажете в Google-форме)."
        )
        await update.message.reply_text(instruction, parse_mode="Markdown")

        context.user_data.clear()
        context.user_data["stage"] = "ask_name"
        return


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    user_id = update.effective_user.id
    text = message.text.strip()
    stage = context.user_data.get("stage")

    if not stage:
        await message.reply_text(
            "Пожалуйста, нажмите /start, чтобы начать или продолжить эксперимент."
        )
        return

    # ---------- Блок общих вопросов ----------
    if stage == "ask_name":
        user = update.effective_user
        context.user_data["participant_name"] = text

        await create_participant(
            user_id=user.id,
            tg_username=user.username,
            first_name=user.first_name,
            participant_name=text,
        )

        context.user_data["stage"] = "ask_gender"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👩 Женский", callback_data="gender_female"),
            InlineKeyboardButton("👨 Мужской", callback_data="gender_male")]
        ])
        await message.reply_text(
            "2️⃣/3️⃣ — Укажите ваш пол:",
            reply_markup=keyboard
        )
        return
    
    await create_participant(
        user_id=user_id,
        tg_username=update.effective_user.username or "",
        first_name=update.effective_user.first_name or "",
        participant_name=text,
        gender=None,
        age=None,
        condition=None,
        total_videos=None,
    )

    if stage == "ask_gender":
        context.user_data["gender"] = text
        context.user_data["stage"] = "ask_age"
        await message.reply_text("3️⃣/3️⃣ Укажите, пожалуйста, ваш возраст (числом).")
        return

    if stage == "ask_age":
        try:
            age = int(text)
            if age <= 0 or age > 120:
                raise ValueError
        except ValueError:
            await message.reply_text(
                "3️⃣/3️⃣ — Пожалуйста, укажите возраст целым числом (например, 25)."
            )
            return

        user = update.effective_user
        participant_name = context.user_data.get("participant_name", "").strip()
        gender = context.user_data.get("gender", "").strip()

        # 1) сохраняем возраст в БД
        await create_participant(
            user_id=user.id,
            tg_username=user.username,
            first_name=user.first_name,
            participant_name=participant_name,
            gender=gender,
            age=age,
        )

        # 2) выбираем условие и сохраняем его отдельно
        condition = random.choice(VIDEO_CONDITIONS)
        total_videos = len(SCENARIOS)

        await create_participant(
            user_id=user.id,
            tg_username=user.username,
            first_name=user.first_name,
            condition=condition,
            total_videos=total_videos,
        )

        await create_video_sequence_for_participant(user.id, condition)

        await message.reply_text(
            "Спасибо! Общие данные записаны.\n\n"
            "Теперь начнётся основная часть эксперимента: вам будет показано *4 видео*.\n\n"
            "Для каждого видео:\n"
            "• Посмотрите видео со звуком.\n"
            "• Опишите, что делает робот.\n"
            "• Вставьте два наречия (как он ведёт себя и как делает выбор).\n"
            "• Оцените, какое из двух утверждений лучше описывает происходящее.\n\n"
            "Начнём с первого видео.",
            parse_mode="Markdown"
        )

        context.user_data["stage"] = None
        await continue_experiment(update, context)
        return


    # ---------- Блок ответов по видео ----------
    participant = await get_participant(user_id)
    if not participant or participant["completed"]:
        await message.reply_text(
            "Сессия эксперимента не найдена или уже завершена. Нажмите /start."
        )
        context.user_data.clear()
        return

    idx = participant["current_video_idx"]
    total = participant["total_videos"]
    video_info = await get_video_by_position(user_id, idx)

    if not video_info:
        await message.reply_text(
            "Ошибка: не найдена информация о текущем видео. "
            "Нажмите /start или обратитесь к организатору."
        )
        return

    scenario = video_info["scenario"]
    file_id = video_info["file_id"]

    if stage == "expect_description":
        await upsert_answer_field(
            user_id,
            idx,
            scenario,
            file_id,
            "description",
            text,
        )
        context.user_data["stage"] = "expect_adv_behavior"
        await message.reply_text(
            "Спасибо.\n\n"
            "3. Вставьте наречие вместо пробела.\n"
            "Робот ведёт себя ____ (Как?)"
        )
        return

    if stage == "expect_adv_behavior":
        await upsert_answer_field(
            user_id,
            idx,
            scenario,
            file_id,
            "adv_behavior",
            text,
        )
        context.user_data["stage"] = "expect_adv_choice"
        await message.reply_text(
            "Спасибо.\n\n"
            "4. Вставьте наречие вместо пробела.\n"
            "Робот делает выбор ____ (Как?)"
        )
        return


    if stage == "expect_adv_choice":
        await upsert_answer_field(
            user_id,
            idx,
            scenario,
            file_id,
            "adv_choice",
            text,
        )
        context.user_data["stage"] = "expect_rating"
        await continue_experiment(update, context)
        return

    if stage == "expect_rating":
        await message.reply_text(
            "Пожалуйста, выберите число от 1 до 10, нажав на одну из кнопок под вопросом."
        )
        return

    await message.reply_text(
        "Похоже, произошла ошибка с определением шага. Нажмите /start, чтобы восстановить сессию."
    )
    context.user_data.clear()

async def handle_gender_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user = q.from_user
    user_id = user.id

    gender = "Женский" if "female" in q.data else "Мужской"
    context.user_data["gender"] = gender
    context.user_data["stage"] = "ask_age"

    await create_participant(
        user_id=user_id,
        tg_username=user.username,
        first_name=user.first_name,
        participant_name=context.user_data.get("participant_name"),
        gender=gender,
    )
    await q.message.edit_text(
        f"2️⃣/3️⃣ Вы указали пол: *{gender}*.\n\n3️⃣/3️⃣ — Укажите, пожалуйста, ваш возраст (числом):",
        parse_mode="Markdown"
    )


async def handle_likert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    try:
        await q.answer()
        data = q.data
        if not data.startswith("likert_"):
            return

        score = int(data.split("_", 1)[1])
        if score < 1 or score > 10:
            raise ValueError

        user_id = q.from_user.id
        participant = await get_participant(user_id)

        if not participant or participant["completed"]:
            await q.message.reply_text(
                "Сессия эксперимента не найдена или уже завершена. Нажмите /start."
            )
            context.user_data.clear()
            return

        idx = participant["current_video_idx"]
        total = participant["total_videos"]
        video_info = await get_video_by_position(user_id, idx)
        if not video_info:
            await q.message.reply_text(
                "Ошибка: не найдена информация о текущем видео. Нажмите /start."
            )
            return

        scenario = video_info["scenario"]
        file_id = video_info["file_id"]

        await upsert_answer_field(
            user_id,
            idx,
            scenario,
            file_id,
            "scenario_rating",
            score,
        )

        try:
            await q.edit_message_reply_markup(reply_markup=None)
        except BadRequest:
            pass

        next_idx = idx + 1
        if next_idx >= total:
            await update_participant_progress(user_id, current_video_idx=next_idx, completed=True)
            participant = await get_participant(user_id)
            await send_final_message(update, participant)
            context.user_data.clear()
        else:
            await update_participant_progress(user_id, current_video_idx=next_idx)
            await q.message.reply_text("Спасибо! Переходим к следующему видео.")
            context.user_data["stage"] = None
            await continue_experiment(update, context)

    except BadRequest as e:
        msg = str(e).lower()
        logger.warning("BadRequest in handle_likert: %s", e)
        if "query is too old" in msg:
            await q.message.reply_text(
                "🕒 Эта кнопка устарела. Нажмите /start, чтобы восстановить сессию."
            )
        else:
            await q.message.reply_text(
                "⚠️ Возникла ошибка при обработке ответа. Нажмите /start, чтобы продолжить."
            )
    except Exception as e:
        logger.exception("Unexpected error in handle_likert: %s", e)
        await q.message.reply_text(
            "⚠️ Произошла неожиданная ошибка. Нажмите /start, чтобы продолжить."
        )


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Сервисная функция: если прислали видео, бот отвечает его file_id.
    Используется для предварительной загрузки и копирования токенов в VIDEO_FILES.
    """
    message = update.message
    if message and message.video:
        await message.reply_text(f"Ваш file_id: {message.video.file_id}")
    else:
        await message.reply_text("Пожалуйста, отправьте видео.")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception while handling update:", exc_info=context.error)
    try:
        chat = getattr(update, "effective_chat", None) if update else None
        if not chat:
            return
        if isinstance(context.error, BadRequest):
            return
        await context.bot.send_message(
            chat_id=chat.id,
            text="⚠️ Произошла ошибка. Пожалуйста, нажмите /start, чтобы продолжить.",
        )
    except Exception:
        logger.exception("Failed to notify user about error")


# ---------------------------------------------------------------------------
#                                  MAIN
# ---------------------------------------------------------------------------

async def main():
    await create_schema_and_fill()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_gender_choice, pattern=r"^gender_"))
    app.add_handler(CallbackQueryHandler(handle_likert, pattern=r"^likert_\d+$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # Сервисный обработчик видео — для получения file_id
    app.add_handler(MessageHandler(filters.VIDEO & ~filters.COMMAND, handle_video))

    app.add_error_handler(error_handler)

    await app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    asyncio.run(main())