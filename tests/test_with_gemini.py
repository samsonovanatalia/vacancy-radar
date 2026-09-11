"""Проверки устойчивости enrich/with_gemini.py: повторы, пропуск сбоев, предохранитель.

Ни Gemini, ни BigQuery тесты не вызывают. Запросы идут через настоящий SDK
google-genai, но его HTTP-транспорт подменён на httpx.MockTransport: какой
ответ вернуть (200, 504, 429, таймаут...), решает сценарий по заголовку
вакансии. Так проверяется весь путь целиком: как SDK разбирает ошибку, как её
видит наш код и сколько запросов насчитал хук-счётчик. Паузы подменены и
по-настоящему не ждут, так что тесты идут секунды.

Запуск из папки проекта:
    python -m unittest tests.test_with_gemini -v

unittest, а не pytest: он входит в стандартную библиотеку, и новая
зависимость в requirements.txt не нужна.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import unittest
from unittest import mock

import httpx
from google import genai

import enrich.with_gemini as eg

# Настоящий конструктор клиента. В тестах genai.Client подменён обёрткой,
# которая вызывает этот конструктор, добавив поддельный транспорт.
REAL_CLIENT = genai.Client

# Ответ модели со всеми полями схемы — такой ask_gemini примет.
ANSWER = {
    "summary": "s", "responsibilities": ["r"], "requirements": ["q"], "stack": ["SQL"],
    "seniority": "mid", "domain": "analytics", "work_mode": "remote",
    "location_city": None, "location_country": None, "residency_requirement": None,
    "salary_min": None, "salary_max": None, "salary_currency": None, "salary_period": None,
    "application_deadline": None, "benefits": [], "language": "en",
}

# Сценарий — ответы на попытки по одной вакансии по порядку; когда список
# кончился, повторяется последний ответ. Заголовок вакансии — имя сценария.
# После # можно дописать что угодно, чтобы завести несколько вакансий с одним
# сценарием: "400#1", "400#2".
SCENARIOS = {
    "ok": ["200"],
    "bad_json": ["bad_json"],
    "504_once": ["504", "200"],
    "504_always": ["504"],
    "502_html_once": ["502_html", "200"],
    "429_minute_once": ["429_minute", "200"],
    "429_daily": ["429_daily"],
    "400": ["400"],
    "403": ["403"],
    "timeout_once": ["read_timeout", "200"],
    "connect_always": ["connect_error"],
    "remote_protocol": ["remote_protocol"],
}


def ok_response(text: str | None = None) -> httpx.Response:
    """Успешный ответ generate_content; text — что вернула модель."""
    if text is None:
        text = json.dumps(ANSWER)
    body = {
        "candidates": [{"content": {"parts": [{"text": text}], "role": "model"},
                        "finishReason": "STOP"}],
        "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 10},
    }
    return httpx.Response(200, json=body)


def api_error(code: int, status: str, message: str, details: list | None = None) -> httpx.Response:
    """Ошибка в том виде, в каком её отдаёт Gemini API."""
    error = {"code": code, "message": message, "status": status}
    if details:
        error["details"] = details
    return httpx.Response(code, json={"error": error})


def quota_429(quota_id: str, value: str, retry_delay: str) -> httpx.Response:
    """429 с описанием превышенной квоты и рекомендованной задержкой."""
    return api_error(429, "RESOURCE_EXHAUSTED", "You exceeded your current quota", [
        {"@type": "type.googleapis.com/google.rpc.QuotaFailure",
         "violations": [{"quotaId": quota_id, "quotaValue": value}]},
        {"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": retry_delay},
    ])


def respond(outcome: str, request: httpx.Request) -> httpx.Response:
    """Превращает имя исхода из SCENARIOS в ответ или сетевую ошибку."""
    if outcome == "200":
        return ok_response()
    if outcome == "bad_json":
        return ok_response("это не JSON")
    if outcome == "504":
        return api_error(504, "DEADLINE_EXCEEDED", "Deadline expired before operation could complete.")
    if outcome == "502_html":
        # Шлюз перед API иногда отвечает HTML-страницей, а не JSON.
        return httpx.Response(502, text="<html><body>Bad Gateway</body></html>")
    if outcome == "429_minute":
        return quota_429("GenerateRequestsPerMinutePerProjectPerModel-FreeTier", "15", "37s")
    if outcome == "429_daily":
        return quota_429("GenerateRequestsPerDayPerProjectPerModel-FreeTier", "500", "3600s")
    if outcome == "400":
        return api_error(400, "INVALID_ARGUMENT", "Request contains an invalid argument.")
    if outcome == "403":
        return api_error(403, "PERMISSION_DENIED", "API key not valid.")
    if outcome == "read_timeout":
        raise httpx.ReadTimeout("The read operation timed out", request=request)
    if outcome == "connect_error":
        raise httpx.ConnectError("[Errno 11001] getaddrinfo failed", request=request)
    if outcome == "remote_protocol":
        raise httpx.RemoteProtocolError("Server disconnected without sending a response.", request=request)
    raise AssertionError(f"неизвестный исход {outcome}")


def make_vacancy(title: str) -> dict:
    """Кандидат в том виде, в каком его возвращает fetch_candidates."""
    return {
        "vacancy_key": f"test:{title}", "title": title, "company_name": None,
        "location": None, "source": "arbeitnow", "job_types": [], "salary_min": None,
        "salary_max": None, "salary_text": None, "description_clean": "text",
    }


class EnrichResilienceTest(unittest.TestCase):

    def setUp(self) -> None:
        self.requests_by_title: dict[str, int] = {}   # заголовок → сколько запросов ушло
        self.sleeps: list[float] = []                 # все паузы по порядку
        self.saved_keys: list[str] = []               # что «записано в BigQuery»
        # patch подменяет объект на время теста, addCleanup возвращает на место,
        # даже если тест упал.
        for patcher in [
            mock.patch.dict(os.environ, {"BQ_PROJECT": "fake-project", "GEMINI_API_KEY": "fake-key"}),
            mock.patch.object(genai, "Client", self.client_with_mock_transport),
            mock.patch.object(eg.bigquery, "Client", lambda project: None),
            mock.patch.object(eg, "ensure_table", lambda bq, table_id: None),
            mock.patch.object(eg, "save", self.fake_save),
            mock.patch.object(eg.time, "sleep", self.sleeps.append),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    # --- подмены ---

    def client_with_mock_transport(self, *, api_key, http_options):
        """genai.Client, у которого вместо сети — handle_request."""
        # У SDK не должно быть своих повторов: все повторы — в ask_with_retries.
        self.assertIsNone(http_options.retry_options)
        http_options.client_args["transport"] = httpx.MockTransport(self.handle_request)
        return REAL_CLIENT(api_key=api_key, http_options=http_options)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Отвечает на запрос к модели по сценарию из заголовка вакансии."""
        text = json.loads(request.content)["contents"][0]["parts"][0]["text"]
        title = next(line.removeprefix("Title: ") for line in text.splitlines()
                     if line.startswith("Title: "))
        attempt = self.requests_by_title.get(title, 0)
        self.requests_by_title[title] = attempt + 1
        outcomes = SCENARIOS[title.split("#")[0]]
        return respond(outcomes[min(attempt, len(outcomes) - 1)], request)

    def fake_save(self, bq, table_id, rows) -> None:
        self.saved_keys.extend(row["vacancy_key"] for row in rows)

    def run_enrich(self, titles: list[str]) -> tuple[str, str | None]:
        """Прогоняет enrich по вакансиям с такими заголовками.

        Возвращает лог и сообщение SystemExit. None вместо сообщения значит,
        что скрипт завершился без SystemExit, то есть с кодом 0. Строка —
        с кодом 1: SystemExit со строкой печатает её и выходит с кодом 1.
        """
        candidates = [make_vacancy(title) for title in titles]
        log = io.StringIO()
        with mock.patch.object(eg, "fetch_candidates", lambda bq, project, limit, source: candidates), \
                contextlib.redirect_stdout(log):
            try:
                eg.enrich(limit=100, source=None)
            except SystemExit as stop:
                return log.getvalue(), stop.code
        return log.getvalue(), None

    # --- тесты ---

    def test_mixed_run(self) -> None:
        """Временные сбои повторяются, остальные пропускаются, прогон доходит до конца."""
        titles = ["ok", "504_once", "504_always", "timeout_once", "connect_always",
                  "502_html_once", "429_minute_once", "400", "403", "bad_json",
                  "remote_protocol", "ok#2"]
        log, exit_message = self.run_enrich(titles)

        self.assertIsNone(exit_message, log)
        # Временные сбои — до RETRY_ATTEMPTS запросов, постоянные — один.
        self.assertEqual(self.requests_by_title, {
            "ok": 1, "504_once": 2, "504_always": 3, "timeout_once": 2, "connect_always": 3,
            "502_html_once": 2, "429_minute_once": 2, "400": 1, "403": 1, "bad_json": 1,
            "remote_protocol": 1, "ok#2": 1,
        })
        self.assertCountEqual(self.saved_keys, [
            f"test:{title}" for title in
            ["ok", "504_once", "timeout_once", "502_html_once", "429_minute_once", "ok#2"]
        ])
        base = eg.RETRY_BASE_SECONDS
        retry_pauses = [seconds for seconds in self.sleeps if seconds != eg.PAUSE_SECONDS]
        # 504_once: 20 | 504_always: 20, 40 | timeout_once: 20 | connect_always: 20, 40 |
        # 502_html_once: 20 | 429_minute_once: большее из своей паузы и retryDelay 37 с.
        self.assertEqual(retry_pauses, [base, base, 2 * base, base, base, 2 * base, base, max(base, 37)])
        self.assertEqual(self.sleeps.count(eg.PAUSE_SECONDS), len(titles))
        self.assertIn("итог: обогащено 6, пропущено 6, запросов к модели 20", log)
        for title in ["504_always", "connect_always", "400", "403", "bad_json", "remote_protocol"]:
            self.assertIn(f"  сбой: test:{title} — ", log)
        # Четыре сбоя подряд (400, 403, bad_json, remote_protocol) — меньше порога.
        self.assertNotIn("остановлен досрочно", log)
        self.assertNotIn("fake-key", log)

    def test_nothing_enriched_exits_with_code_1(self) -> None:
        """Не обогатилась ни одна — код 1, но все вакансии обработаны и итог напечатан."""
        log, exit_message = self.run_enrich(["400", "403", "504_always"])

        self.assertIsInstance(exit_message, str, log)
        self.assertIn("ни одна вакансия из 3", exit_message)
        self.assertIn("итог: обогащено 0, пропущено 3, запросов к модели 5", log)

    def test_daily_quota_stops_run(self) -> None:
        """Суточная квота останавливает прогон; готовое записано, итог напечатан."""
        log, exit_message = self.run_enrich(["ok", "ok#2", "429_daily", "ok#3"])

        self.assertIsInstance(exit_message, str, log)
        self.assertIn("Суточная квота", exit_message)
        self.assertIn("500", exit_message)
        self.assertCountEqual(self.saved_keys, ["test:ok", "test:ok#2"])
        self.assertNotIn("ok#3", self.requests_by_title)
        self.assertIn("итог: обогащено 2, пропущено 0, запросов к модели 3", log)

    def test_breaker_stops_after_failures_in_a_row(self) -> None:
        """MAX_CONSECUTIVE_FAILURES сбоев подряд — остановка; одну обогатили — код 0."""
        limit = eg.MAX_CONSECUTIVE_FAILURES
        failures = [f"400#{i}" for i in range(limit)]
        log, exit_message = self.run_enrich(["ok", *failures, "ok#2"])

        self.assertIn("прогон остановлен досрочно", log)
        self.assertIn("не обработано вакансий: 1", log)
        self.assertNotIn("ok#2", self.requests_by_title)
        self.assertIsNone(exit_message, log)
        self.assertEqual(self.saved_keys, ["test:ok"])
        self.assertIn(f"итог: обогащено 1, пропущено {limit}, запросов к модели {limit + 1}", log)

    def test_breaker_with_nothing_enriched_exits_with_code_1(self) -> None:
        """Досрочная остановка без единого успеха — код 1, итог напечатан."""
        limit = eg.MAX_CONSECUTIVE_FAILURES
        log, exit_message = self.run_enrich([f"400#{i}" for i in range(limit + 1)])

        self.assertIn("прогон остановлен досрочно", log)
        self.assertEqual(len(self.requests_by_title), limit)
        self.assertIsInstance(exit_message, str, log)
        self.assertIn(f"итог: обогащено 0, пропущено {limit}, запросов к модели {limit}", log)

    def test_success_resets_breaker(self) -> None:
        """Разрозненные сбои не копятся: успех между ними обнуляет счёт."""
        almost = eg.MAX_CONSECUTIVE_FAILURES - 1
        titles = [*(f"400#a{i}" for i in range(almost)), "ok",
                  *(f"400#b{i}" for i in range(almost)), "ok#2"]
        log, exit_message = self.run_enrich(titles)

        self.assertNotIn("остановлен досрочно", log)
        self.assertEqual(len(self.requests_by_title), len(titles))
        self.assertIsNone(exit_message, log)
        self.assertIn(f"итог: обогащено 2, пропущено {2 * almost}", log)


if __name__ == "__main__":
    unittest.main()
