#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Экспорт результатов эксперимента из SQLite-базы (ratings.db) в Excel-файл.

Структура Excel-файла:
- Лист "participants" — участники (личные данные и статус).
- Лист "answers"      — ответы по каждому видео для каждого участника.

Запуск:
    python export_to_excel.py

Файл будет сохранён как:
    <корень проекта>/data/experiment_results.xlsx
"""

import sqlite3
from pathlib import Path

import pandas as pd


# Пути — такие же уровнем, как в tg_bot.py
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "ratings.db"
OUT_PATH = BASE_DIR / "data" / "experiment_results.xlsx"


def load_participants(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Загрузить краткий список участников:
    user_id, tg_username, first_name, participant_name, gender, age, condition, completed, created_at.
    """
    query = """
    SELECT
        user_id,
        tg_username,
        first_name,
        participant_name,
        gender,
        age,
        condition,
        completed,
        created_at
    FROM participants
    ORDER BY user_id;
    """
    return pd.read_sql_query(query, conn)


def load_answers(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Собрать таблицу ответов:
    одна строка = один пользователь × одно видео.

    Поля:
    - user_id
    - tg_username
    - first_name
    - participant_name
    - gender
    - age
    - condition
    - video_position
    - video_scenario
    - answer_description
    - answer_adv_behavior
    - answer_adv_choice
    - answer_scenario_rating

    Специально НЕ включаем:
    - video_file_id (file_id)
    - total_videos
    - current_video_idx
    - completed
    """
    query = """
    SELECT
        p.user_id,
        p.tg_username,
        p.first_name,
        p.participant_name,
        p.gender,
        p.age,
        p.condition,

        v.position        AS video_position,
        v.scenario        AS video_scenario,

        a.description     AS answer_description,
        a.adv_behavior    AS answer_adv_behavior,
        a.adv_choice      AS answer_adv_choice,
        a.scenario_rating AS answer_scenario_rating

    FROM participants p
    LEFT JOIN video_sequence v
        ON v.user_id = p.user_id
    LEFT JOIN answers a
        ON a.user_id = p.user_id
       AND a.position = v.position

    ORDER BY p.user_id, v.position;
    """
    return pd.read_sql_query(query, conn)


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"База данных не найдена: {DB_PATH}")

    print(f"Открываю БД: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        print("Читаю таблицу участников...")
        participants_df = load_participants(conn)

        print("Собираю таблицу ответов...")
        answers_df = load_answers(conn)

    finally:
        conn.close()

    # Убедимся, что папка data существует
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Сохраняю Excel-файл: {OUT_PATH}")
    with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as writer:
        participants_df.to_excel(writer, sheet_name="participants", index=False)
        answers_df.to_excel(writer, sheet_name="answers", index=False)

    print("Готово ✅")


if __name__ == "__main__":
    main()
