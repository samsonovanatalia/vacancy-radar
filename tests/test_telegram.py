"""Проверки notify/telegram.py: что уходит в Telegram и что пишется в raw.digest_sent.

Ни Telegram, ни BigQuery тесты не вызывают. HTTP-клиент httpx настоящий, но
его транспорт подменён на httpx.MockTransport: ответ выбирается по сценарию
из ссылки на вакансию в тексте сообщения (https://example.com/jobs/<сценарий>).
Запись в raw.digest_sent подменена списком, паузы по-настоящему не ждут.

Запрос к очереди (SQL) здесь не проверяется. Его подменяет список, из
которого убраны уже записанные вакансии — как делает not exists в настоящем
запросе. Поэтому несколько вызовов run_send подряд — это несколько запусков
бота в разные дни.

Запуск из папки проекта:
    python -m unittest tests.test_telegram -v
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import unittest
from unittest import mock

import httpx

import notify.telegram as tg

# Настоящий конструктор клиента: в тестах httpx.Client подменён обёрткой,
# которая вызывает его, добавив поддельный транспорт.
REAL_HTTPX_CLIENT = httpx.Client

TOKEN = "123456:fake-token"
CHAT_ID = "42"

# Сколько секунд просит подождать Telegram в ответе 429.
RETRY_AFTER = 7

# Сколько попыток делает send_message на одно сообщение.
ATTEMPTS = len(tg.RETRY_PAUSES) + 1

# Сценарий — ответы на попытки отправить одно сообщение по порядку; когда
# список кончился, повторяется последний. Счёт попыток общий на все запуски
# в тесте. После # можно дописать что угодно, чтобы завести несколько
# вакансий с одним сценарием: "ok", "ok#2".
SCENARIOS = {
    "ok": ["ok"],
    "400": ["400"],
    "400_chat": ["400_chat"],
    "403": ["403"],
    "429_once": ["429", "ok"],
    "429_always": ["429"],
    "502_always": ["502"],
    # Все попытки первого запуска — 502, первая же попытка следующего — ok.
    "502_until_next_run": ["502"] * ATTEMPTS + ["ok"],
}


def telegram_error(code: int, description: str, parameters: dict | None = None) -> httpx.Response:
    """Ошибка в том виде, в каком её отдаёт Bot API."""
    body = {"ok": False, "error_code": code, "description": description}
    if parameters:
        body["parameters"] = parameters
    return httpx.Response(code, json=body)


def respond(outcome: str) -> httpx.Response:
    """Превращает имя исхода из SCENARIOS в ответ Telegram."""
    if outcome == "ok":
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}})
    if outcome == "400":
        return telegram_error(400, "Bad Request: can't parse entities: Unsupported start tag at byte offset 3")
    if outcome == "400_chat":
        return telegram_error(400, "Bad Request: chat not found")
    if outcome == "403":
        return telegram_error(403, "Forbidden: bot was blocked by the user")
    if outcome == "429":
        return telegram_error(429, f"Too Many Requests: retry after {RETRY_AFTER}",
                              {"retry_after": RETRY_AFTER})
    if outcome == "502":
        # Шлюз перед API отвечает HTML-страницей, а не JSON.
        return httpx.Response(502, text="<html><body>502 Bad Gateway</body></html>")
    raise AssertionError(f"неизвестный исход {outcome}")


def make_vacancy(scenario: str, **fields) -> dict:
    """Строка очереди в том виде, в каком её возвращает fetch_queue.

    По умолчанию заполнены только заголовок и ссылка — всё остальное пустое,
    как у необогащённой вакансии. Нужные поля передаются аргументами.
    """
    vacancy = {
        "vacancy_key": f"test:{scenario}",
        "title": "Data Analyst",
        "company_name": None,
        "location_city": None,
        "location_country": None,
        "work_mode": None,
        "seniority": None,
        "stack": [],
        "salary_min": None,
        "salary_max": None,
        "salary_currency": None,
        "salary_period": None,
        "residency_requirement": None,
        "summary": None,
        "url": f"https://example.com/jobs/{scenario}",
    }
    vacancy.update(fields)
    return vacancy


class SendDigestTest(unittest.TestCase):

    def setUp(self) -> None:
        self.payloads: list[dict] = []                  # тела запросов по порядку, с повторами
        self.requests_by_scenario: dict[str, int] = {}  # сценарий → сколько запросов ушло
        self.header_outcomes = ["ok"]                   # ответы на шапку и сообщение пустого дня
        self.sleeps: list[float] = []
        self.saved: list[tuple[str, str]] = []          # что «записано в raw.digest_sent»: (ключ, status)
        for patcher in [
            mock.patch.dict(os.environ, {
                "BQ_PROJECT": "fake-project",
                "TELEGRAM_BOT_TOKEN": TOKEN,
                "TELEGRAM_CHAT_ID": CHAT_ID,
            }),
            mock.patch.object(tg.httpx, "Client", self.client_with_mock_transport),
            mock.patch.object(tg.bigquery, "Client", lambda project: None),
            mock.patch.object(tg, "save_status",
                              lambda bq, table_id, key, status: self.saved.append((key, status))),
            mock.patch.object(tg.time, "sleep", self.sleeps.append),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    # --- подмены ---

    def client_with_mock_transport(self, **kwargs) -> httpx.Client:
        """httpx.Client, у которого вместо сети — handle_request."""
        return REAL_HTTPX_CLIENT(transport=httpx.MockTransport(self.handle_request), **kwargs)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Отвечает по сценарию из ссылки в сообщении; сообщение без ссылки — шапка."""
        self.assertEqual(request.url.path, f"/bot{TOKEN}/sendMessage")
        payload = json.loads(request.content)
        self.payloads.append(payload)

        link = re.search(r'href="https://example\.com/jobs/([^"?]+)', payload["text"])
        if link is None:
            scenario, outcomes = "header", self.header_outcomes
        else:
            scenario = link.group(1)
            outcomes = SCENARIOS[scenario.split("#")[0]]
        attempt = self.requests_by_scenario.get(scenario, 0)
        self.requests_by_scenario[scenario] = attempt + 1
        return respond(outcomes[min(attempt, len(outcomes) - 1)])

    def run_send(self, queue: list[dict]) -> tuple[str, str | None]:
        """Один запуск бота. Возвращает лог и сообщение SystemExit (None — код 0).

        fetch_queue отдаёт очередь без вакансий, уже записанных в self.saved.
        """
        recorded = {key for key, _ in self.saved}
        remaining = [vacancy for vacancy in queue if vacancy["vacancy_key"] not in recorded]
        log = io.StringIO()
        with mock.patch.object(tg, "fetch_queue", lambda bq, project: remaining), \
                contextlib.redirect_stdout(log):
            try:
                tg.send_digest()
            except SystemExit as stop:
                return log.getvalue(), stop.code
        return log.getvalue(), None

    def texts(self) -> list[str]:
        return [payload["text"] for payload in self.payloads]

    # --- отправка ---

    def test_success(self) -> None:
        """Шапка и вакансии доставлены, обе записаны как sent; пустые поля не печатаются."""
        full = make_vacancy(
            "ok",
            title="R&D <Data> Analyst",
            company_name="Zynga",
            location_city="Barcelona",
            location_country="ES",
            work_mode="hybrid",
            seniority="senior",
            stack=["SQL", "dbt"],
            salary_min=45000.0,
            salary_max=55000.0,
            salary_currency="EUR",
            salary_period="year",
            residency_requirement="Must be based in Spain.",
            summary="Build dashboards. Write SQL. Talk to stakeholders.",
            url="https://example.com/jobs/ok?utm_medium=api&utm_source=x",
        )
        sparse = make_vacancy(
            "ok#2",
            company_name="Perk",
            work_mode="unclear",
            seniority="unclear",
            salary_min=35000.0,
            salary_currency="EUR",
        )
        log, exit_message = self.run_send([full, sparse])

        self.assertIsNone(exit_message, log)
        header, full_text, sparse_text = self.texts()
        # Очередь не длиннее лимита — «из» и «остальные завтра» в шапке нет.
        self.assertRegex(header, r"^<b>Дайджест за \d\d\.\d\d\.\d{4}</b>, вакансий: 2$")
        # Заголовок экранирован, суть — два предложения, & в ссылке — &amp;.
        self.assertEqual(full_text, (
            "<b>R&amp;D &lt;Data&gt; Analyst</b>\n"
            "Компания: Zynga\n"
            "Город: Barcelona, ES\n"
            "Формат работы: гибрид\n"
            "Грейд: senior\n"
            "Стек: SQL, dbt\n"
            "Зарплата: 45 000–55 000 EUR в год\n"
            "Резидентство: Must be based in Spain.\n"
            "\n"
            "Build dashboards. Write SQL.\n"
            "\n"
            '<a href="https://example.com/jobs/ok?utm_medium=api&amp;utm_source=x">Открыть вакансию</a>'
        ))
        # unclear, пустой стек и пустые поля не печатаются совсем — ни «—», ни «None».
        self.assertEqual(sparse_text, (
            "<b>Data Analyst</b>\n"
            "Компания: Perk\n"
            "Зарплата: от 35 000 EUR\n"
            "\n"
            '<a href="https://example.com/jobs/ok#2">Открыть вакансию</a>'
        ))
        for payload in self.payloads:
            self.assertEqual(payload["chat_id"], CHAT_ID)
            self.assertEqual(payload["parse_mode"], "HTML")
            self.assertEqual(payload["link_preview_options"], {"is_disabled": True})
        self.assertEqual(self.saved, [("test:ok", "sent"), ("test:ok#2", "sent")])
        self.assertIn("итог: в очереди 2, отправляли 2: доставлено 2, недоставляемых 0, сбоев 0; "
                      "шапка доставлена; осталось в очереди 0", log)
        self.assertNotIn(TOKEN, log)

    def test_one_message_fails(self) -> None:
        """Временный сбой на одном сообщении: остальные доставлены, сбойная не записана."""
        log, exit_message = self.run_send([make_vacancy("ok"), make_vacancy("502_always"), make_vacancy("ok#2")])

        self.assertIsNone(exit_message, log)
        # Порядок: шапка, потом вакансии, как пришли из очереди.
        self.assertEqual(list(self.requests_by_scenario), ["header", "ok", "502_always", "ok#2"])
        self.assertEqual(self.requests_by_scenario["502_always"], ATTEMPTS)
        self.assertEqual(self.saved, [("test:ok", "sent"), ("test:ok#2", "sent")])
        self.assertIn("доставлено 2, недоставляемых 0, сбоев 1; шапка доставлена; осталось в очереди 1", log)
        self.assertIn(f"  сбой: test:502_always — код 502: ответ без описания; не помогли {ATTEMPTS} попытки", log)

    def test_telegram_429(self) -> None:
        """429 повторяется с паузой не меньше retry_after; не помогли повторы — не записываем."""
        log, exit_message = self.run_send([make_vacancy("429_once"), make_vacancy("429_always")])

        self.assertEqual(self.requests_by_scenario["429_once"], 2)
        self.assertEqual(self.requests_by_scenario["429_always"], ATTEMPTS)
        retry_pauses = [seconds for seconds in self.sleeps if seconds != tg.PAUSE_SECONDS]
        # Каждая пауза — большее из своей и retry_after. 429_once: одна пауза,
        # 429_always: столько, сколько повторов.
        expected = [max(tg.RETRY_PAUSES[0], RETRY_AFTER),
                    *(max(pause, RETRY_AFTER) for pause in tg.RETRY_PAUSES)]
        self.assertEqual(retry_pauses, expected)
        # 429 — временный сбой: undeliverable не пишется, вакансия остаётся в очереди.
        self.assertEqual(self.saved, [("test:429_once", "sent")])
        self.assertIsNone(exit_message, log)
        self.assertIn("  сбой: test:429_always — код 429: Too Many Requests", log)

    def test_nothing_delivered_exits_with_code_1(self) -> None:
        """Шапка ушла, а ни одна вакансия — нет: код 1; недоставляемая доставкой не считается."""
        log, exit_message = self.run_send([make_vacancy("400"), make_vacancy("403")])

        self.assertIsInstance(exit_message, str, log)
        self.assertIn("ни одна вакансия из 2", exit_message)
        self.assertEqual(self.requests_by_scenario["header"], 1)
        # 403 — сломан доступ, а не сообщение: не пишем ничего.
        self.assertEqual(self.saved, [("test:400", "undeliverable")])
        self.assertIn("доставлено 0, недоставляемых 1, сбоев 1; шапка доставлена", log)

    # --- пустая очередь ---

    def test_empty_digest(self) -> None:
        """Пустая очередь — одно сообщение «сегодня новых вакансий нет», код 0."""
        log, exit_message = self.run_send([])

        self.assertIsNone(exit_message, log)
        (text,) = self.texts()
        self.assertRegex(text, r"^<b>Дайджест за \d\d\.\d\d\.\d{4}</b>: сегодня новых вакансий нет$")
        self.assertEqual(self.saved, [])

    def test_empty_digest_not_delivered_exits_with_code_1(self) -> None:
        """Очередь пуста, и сообщение об этом не ушло — код 1: иначе тишина неотличима от поломки."""
        self.header_outcomes = ["403"]
        log, exit_message = self.run_send([])

        self.assertIsInstance(exit_message, str, log)
        self.assertIn("Forbidden: bot was blocked by the user", exit_message)

    # --- лимит ---

    def test_queue_larger_than_limit(self) -> None:
        """Очередь длиннее лимита: уходят первые MAX_PER_RUN, хвост не трогается, шапка это говорит."""
        limit = tg.MAX_PER_RUN
        queue = [make_vacancy(f"ok#{i}") for i in range(limit + 5)]
        log, exit_message = self.run_send(queue)

        self.assertIsNone(exit_message, log)
        self.assertRegex(self.texts()[0],
                         rf"^<b>Дайджест за \d\d\.\d\d\.\d{{4}}</b>, вакансий: {limit} из {limit + 5}, остальные завтра$")
        # Ушли первые limit вакансий в порядке очереди; хвост не запрашивался.
        self.assertEqual(list(self.requests_by_scenario), ["header", *(f"ok#{i}" for i in range(limit))])
        # Записаны только отправленные: о хвосте в raw.digest_sent ничего нет.
        self.assertEqual(self.saved, [(f"test:ok#{i}", "sent") for i in range(limit)])
        self.assertIn(f"итог: в очереди {limit + 5}, отправляли {limit}: доставлено {limit}", log)
        self.assertIn("осталось в очереди 5", log)

    def test_limit_defers_and_loses_nothing(self) -> None:
        """Лимит откладывает: за несколько запусков каждая вакансия очереди доставлена ровно один раз."""
        limit = tg.MAX_PER_RUN
        # Первая в очереди в первый запуск не дойдёт (502), остальные — ok.
        queue = [make_vacancy("502_until_next_run"), *(make_vacancy(f"ok#{i}") for i in range(limit + 4))]

        first_log, first_exit = self.run_send(queue)
        second_log, second_exit = self.run_send(queue)
        third_log, third_exit = self.run_send(queue)

        for log, exit_message in [(first_log, first_exit), (second_log, second_exit), (third_log, third_exit)]:
            self.assertIsNone(exit_message, log)
        # Первый запуск: из limit отправленных одна не дошла; в очереди остались
        # она и 5 вакансий за лимитом.
        self.assertIn(f"итог: в очереди {limit + 5}, отправляли {limit}: доставлено {limit - 1}, "
                      f"недоставляемых 0, сбоев 1", first_log)
        self.assertIn("осталось в очереди 6", first_log)
        # Второй запуск: остаток целиком, и первой — вакансия, не дошедшая в прошлый раз.
        self.assertIn("в очереди: 6, отправляем: 6", second_log)
        self.assertIn("1/6 test:502_until_next_run доставлена", second_log)
        self.assertIn("осталось в очереди 0", second_log)
        # Третий запуск: очередь пуста.
        self.assertIn("сегодня новых вакансий нет", self.texts()[-1])
        # Ничего не потеряно и ничего не задвоено: каждая вакансия записана один раз, как sent.
        self.assertCountEqual(self.saved, [(vacancy["vacancy_key"], "sent") for vacancy in queue])

    # --- undeliverable ---

    def test_400_is_undeliverable(self) -> None:
        """400 — undeliverable: записывается сразу, не повторяется и больше не занимает очередь."""
        queue = [make_vacancy("400"), make_vacancy("ok")]
        log, exit_message = self.run_send(queue)

        self.assertIsNone(exit_message, log)
        self.assertEqual(self.requests_by_scenario["400"], 1)
        self.assertEqual(self.saved, [("test:400", "undeliverable"), ("test:ok", "sent")])
        self.assertIn("итог: в очереди 2, отправляли 2: доставлено 1, недоставляемых 1, сбоев 0; "
                      "шапка доставлена; осталось в очереди 0", log)
        self.assertIn("  недоставляемая: test:400 — код 400: Bad Request: can't parse entities", log)

        # Следующий запуск её не берёт: очередь пуста, запроса по ней больше нет.
        self.run_send(queue)
        self.assertEqual(self.requests_by_scenario["400"], 1)
        self.assertIn("сегодня новых вакансий нет", self.texts()[-1])

    def test_400_with_failed_header_is_not_undeliverable(self) -> None:
        """Шапка тоже получила 400 — дело не в сообщениях (chat not found): ничего не пишем."""
        self.header_outcomes = ["400_chat"]
        log, exit_message = self.run_send([make_vacancy("400_chat"), make_vacancy("400")])

        self.assertEqual(self.saved, [])
        self.assertIsInstance(exit_message, str, log)
        self.assertIn("доставлено 0, недоставляемых 0, сбоев 2; шапка не доставлена; осталось в очереди 2", log)


if __name__ == "__main__":
    unittest.main()
