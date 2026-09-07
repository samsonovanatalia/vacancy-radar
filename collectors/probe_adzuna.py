"""
Разведка перед четвёртым коллектором: что вообще отдаёт Adzuna.

Это не коллектор. Ничего не сохраняет и ничего не чистит — делает
ровно один запрос и печатает две вещи:
  - сколько всего вакансий нашлось по запросу (поле count),
  - какие поля есть у ОДНОЙ вакансии (только имена, без значений).

Зачем отдельный файл: прежде чем писать разбор полей, надо увидеть,
как источник называет свои поля. У arbeitnow, remoteok и remotive
они назывались по-разному, Adzuna — четвёртый вариант.

Ключи берутся из переменных окружения ADZUNA_APP_ID и ADZUNA_APP_KEY.
В коде их нет и не будет — правило проекта.

Запуск:
    python -m collectors.probe_adzuna
"""

from __future__ import annotations

import os

import requests

# Адрес поиска. Структура: .../jobs/{страна}/search/{номер страницы}.
# gb — Великобритания: у Adzuna по каждой стране свой раздел,
# и gb один из самых наполненных.
API_URL = "https://api.adzuna.com/v1/api/jobs/gb/search/1"


def read_credentials() -> tuple[str, str]:
    """Достаёт ключи из окружения.

    os.environ — словарь переменных окружения процесса. Обращаемся через [],
    а не .get(): если переменной нет, пусть скрипт упадёт сразу и внятно,
    а не уйдёт в запрос с пустым ключом и вернёт непонятную 401.
    """
    try:
        return os.environ["ADZUNA_APP_ID"], os.environ["ADZUNA_APP_KEY"]
    except KeyError as error:
        raise SystemExit(
            f"Не задана переменная окружения {error.args[0]}. "
            "Задайте её в терминале перед запуском."
        ) from error


def main() -> None:
    app_id, app_key = read_credentials()

    # Параметры отдаём словарём, а не склеиваем в строку руками:
    # requests сам соберёт запрос, а ключи останутся значениями переменных.
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": 1,   # для разведки хватит одной вакансии
        "what": "data analyst",  # что ищем — пока просто чтобы запрос был осмысленным
    }

    response = requests.get(API_URL, params=params, timeout=30)

    # ВАЖНО: не используем raise_for_status() и не печатаем response.url —
    # в тексте URL лежит ключ целиком, а он не должен попасть ни в вывод,
    # ни в логи. Поэтому сообщаем только код ответа.
    if response.status_code != 200:
        raise SystemExit(f"Adzuna ответила кодом {response.status_code}")

    payload = response.json()

    # count — сколько всего вакансий нашлось по запросу (не сколько пришло).
    print("Всего найдено:", payload["count"])

    vacancies = payload["results"]
    if not vacancies:
        raise SystemExit("По этому запросу вакансий нет — смотреть нечего.")

    # sorted() — чтобы список полей был в одном и том же порядке
    # при каждом запуске и его удобно было сравнивать глазами.
    print("Поля одной вакансии:")
    for field in sorted(vacancies[0]):
        print("  -", field)


if __name__ == "__main__":
    main()
