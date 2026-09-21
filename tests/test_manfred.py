"""Проверки collectors/manfred.py: отбор по заголовку, сборка описания, устойчивость.

Сеть не вызывается: requests.get подменён заглушкой. Паузы подменены и
по-настоящему не ждут. Файл пишется во временную папку теста.

Запуск из папки проекта:
    python -m unittest tests.test_manfred -v
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests

import collectors.manfred as mf

# Оффер в том виде, в каком он приходит в СПИСКЕ: описания здесь нет.
OFFER = {
    "id": 8476,
    "position": "Data Analyst ",          # хвостовой пробел — как у источника
    "slug": "acme-data-analyst-sept26",
    "status": "ACTIVE",
    "salaryFrom": 35000,
    "salaryTo": 45000,
    "remotePercentage": 100,
    "currency": "€",
    "locations": ["Madrid, España"],
    "offerLanguages": ["ES"],
    "updatedAt": "2026-09-18T07:48:11.546Z",
    "company": {"name": "Acme Data", "web": "https://acme.example"},
    "isFreelance": False,
}

# Карточка того же оффера: описание разложено по секциям.
CARD = {
    "introduction": "Acme busca un Data Analyst.",
    "whatWillYouDo": "Construir informes en dbt y BigQuery.",
    "responsibilities": ["Modelar datos", "Mantener dashboards"],
    "whatTheyAskFor": "SQL, Python.",
    "whoWillDoItWith": "",               # пустая секция — в описание не идёт
    "faq": [{"question": "¿Remoto?", "answer": "Sí, 100%."}],
    "workingDayInfo": {"isFullTime": True},
    "techs": [{"name": "SQL"}, {"name": "dbt"}],
    "languages": [{"code": "EN", "name": "Inglés", "level": "Fluent"}],
    "scout": {"firstName": "Lucía", "email": "lucia@getmanfred.com"},
}


class FakeResponse:
    def __init__(self, status_code: int, body=None, text_body: str | None = None):
        self.status_code = status_code
        self._body = body
        self._text = text_body

    def json(self):
        if self._text is not None:
            raise ValueError("не JSON")
        return self._body


class RoleFilterTest(unittest.TestCase):
    """Отбор по заголовку: та же регулярка, что в витрине."""

    def test_data_roles_match(self) -> None:
        for title in ["Data Analyst", "Senior Data Engineer", "Analytics Engineer",
                      "Business Intelligence Specialist", "BI Analyst"]:
            with self.subTest(title=title):
                self.assertTrue(mf.is_data_role({"position": title}))

    def test_other_roles_do_not_match(self) -> None:
        for title in ["Backend Engineer (PHP)", "Product Manager", "Camarero"]:
            with self.subTest(title=title):
                self.assertFalse(mf.is_data_role({"position": title}))

    def test_word_boundaries(self) -> None:
        """«Big Data Engineering Manager» — не data engineer: границы слов на месте."""
        self.assertFalse(mf.is_data_role({"position": "Big Data Engineering Manager"}))

    def test_trailing_space_and_case(self) -> None:
        """У источника в названиях хвостовой пробел и разный регистр."""
        self.assertTrue(mf.is_data_role({"position": "  DATA ANALYST  "}))

    def test_missing_position(self) -> None:
        self.assertFalse(mf.is_data_role({}))


class DescriptionTest(unittest.TestCase):
    """Сборка описания из секций."""

    def test_sections_joined_with_captions(self) -> None:
        text = mf.build_description(CARD)
        self.assertIn("## Вступление", text)
        self.assertIn("Acme busca un Data Analyst.", text)
        self.assertIn("## Что просят", text)

    def test_list_section_becomes_bullets(self) -> None:
        """responsibilities приходит списком строк."""
        text = mf.build_description(CARD)
        self.assertIn("* Modelar datos", text)
        self.assertIn("* Mantener dashboards", text)

    def test_empty_sections_skipped(self) -> None:
        """Пустая секция не даёт пустого заголовка в тексте."""
        self.assertNotIn("## С кем", mf.build_description(CARD))

    def test_faq_included(self) -> None:
        """В FAQ лежит самое полезное — можно ли работать не из Испании."""
        text = mf.build_description(CARD)
        self.assertIn("## FAQ", text)
        self.assertIn("¿Remoto? — Sí, 100%.", text)

    def test_no_sections_gives_none(self) -> None:
        self.assertIsNone(mf.build_description({}))

    def test_base64_image_cut(self) -> None:
        """Вшитая картинка вырезается, отметка о ней остаётся.

        В одной карточке такая картинка занимала 71 781 символ при 150
        символах текста.
        """
        blob = "A" * 5000
        card = {"whereWillDoIt": f'Remoto. <img src="data:image/jpeg;base64,{blob}">'}
        text = mf.build_description(card)
        self.assertNotIn(blob, text)
        self.assertIn("картинка base64", text)
        self.assertIn("Remoto.", text)
        self.assertLess(len(text), 200)

    def test_scout_not_in_description(self) -> None:
        """Почта рекрутера — чужие персональные данные, в raw не идёт."""
        text = mf.build_description(CARD)
        self.assertNotIn("lucia@getmanfred.com", text)


class NormalizeTest(unittest.TestCase):
    """Приведение к общей схеме."""

    def setUp(self) -> None:
        self.row = mf.normalize(OFFER, CARD)

    def test_schema_matches_other_collectors(self) -> None:
        """Набор и ПОРЯДОК полей как у остальных: union all сопоставляет по порядку."""
        self.assertEqual(list(self.row), [
            "source", "source_id", "title", "company_name", "location",
            "remote", "url", "tags", "job_types", "description",
            "created_at_unix", "ingested_at", "source_page",
            "salary_min", "salary_max", "salary_text",
            "salary_currency", "remote_percentage", "languages",
            "offer_languages", "slug",
        ])

    def test_fields(self) -> None:
        self.assertEqual(self.row["source"], "manfred")
        self.assertEqual(self.row["source_id"], "8476")
        self.assertEqual(self.row["title"], "Data Analyst")   # пробел снят
        self.assertEqual(self.row["company_name"], "Acme Data")
        self.assertEqual(self.row["location"], "Madrid, España")
        self.assertEqual(self.row["tags"], ["SQL", "dbt"])
        self.assertEqual(self.row["job_types"], ["full_time"])
        self.assertEqual(self.row["source_page"], 1)

    def test_url(self) -> None:
        self.assertEqual(
            self.row["url"],
            "https://www.getmanfred.com/es/job-offers/8476/acme-data-analyst-sept26")

    def test_remote_from_percentage(self) -> None:
        """100% — удалённая; 40% — нет; поля нет — None, а не False."""
        self.assertIs(self.row["remote"], True)
        self.assertEqual(self.row["remote_percentage"], 100)
        hybrid = mf.normalize({**OFFER, "remotePercentage": 40}, CARD)
        self.assertIs(hybrid["remote"], False)
        unknown = mf.normalize({**OFFER, "remotePercentage": None}, CARD)
        self.assertIsNone(unknown["remote"])

    def test_salary(self) -> None:
        self.assertEqual(self.row["salary_min"], 35000)
        self.assertEqual(self.row["salary_max"], 45000)
        self.assertEqual(self.row["salary_currency"], "€")

    def test_zero_salary_is_not_a_salary(self) -> None:
        """Ноль у источника означает «не указано», а не «ноль евро»."""
        row = mf.normalize({**OFFER, "salaryFrom": 0, "salaryTo": 0}, CARD)
        self.assertIsNone(row["salary_min"])
        self.assertIsNone(row["salary_max"])

    def test_empty_locations(self) -> None:
        """Полностью удалённая вакансия без места работы: None, а не пустая строка."""
        row = mf.normalize({**OFFER, "locations": []}, CARD)
        self.assertIsNone(row["location"])

    def test_languages_kept_with_level(self) -> None:
        self.assertEqual(self.row["languages"], [{"code": "EN", "level": "Fluent"}])

    def test_updated_at_to_unix(self) -> None:
        self.assertEqual(self.row["created_at_unix"], 1789717691)

    def test_freelance_in_job_types(self) -> None:
        row = mf.normalize({**OFFER, "isFreelance": True}, CARD)
        self.assertEqual(row["job_types"], ["full_time", "freelance"])


class FetchJsonTest(unittest.TestCase):
    """Один запрос: повторы при временных сбоях, отказ при постоянных."""

    def setUp(self) -> None:
        self.sleeps: list[float] = []
        patcher = mock.patch.object(mf.time, "sleep", self.sleeps.append)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_with(self, responses: list):
        self.calls = 0

        def fake_get(url, params=None, timeout=None):
            self.calls += 1
            answer = responses[self.calls - 1]
            if isinstance(answer, Exception):
                raise answer
            return answer

        with mock.patch.object(mf.requests, "get", fake_get):
            with contextlib.redirect_stdout(io.StringIO()):
                return mf.fetch_json("https://example.test", "тест")

    def test_success(self) -> None:
        self.assertEqual(self.run_with([FakeResponse(200, {"ok": 1})]), {"ok": 1})

    def test_lang_is_sent(self) -> None:
        """Без lang API отвечает 400 — параметр обязателен."""
        seen = {}

        def fake_get(url, params=None, timeout=None):
            seen.update(params or {})
            return FakeResponse(200, [])

        with mock.patch.object(mf.requests, "get", fake_get):
            mf.fetch_json("https://example.test", "тест")
        self.assertEqual(seen["lang"], "ES")

    def test_non_json_body_is_retried(self) -> None:
        """Код 200, а в теле HTML: у Manfred так отвечает защита от частых запросов."""
        body = self.run_with([FakeResponse(200, text_body="<html>"),
                              FakeResponse(200, {"ok": 1})])
        self.assertEqual(body, {"ok": 1})
        self.assertEqual(self.sleeps, [5])

    def test_temporary_error_retried(self) -> None:
        self.run_with([FakeResponse(503), FakeResponse(200, {"ok": 1})])
        self.assertEqual(self.sleeps, [5])

    def test_permanent_error_not_retried(self) -> None:
        with self.assertRaises(mf.QueryFailed):
            self.run_with([FakeResponse(404)])
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.sleeps, [])

    def test_retries_run_out(self) -> None:
        with self.assertRaises(mf.QueryFailed):
            self.run_with([FakeResponse(503)] * 4)
        self.assertEqual(self.sleeps, [5, 15, 45])

    def test_list_endpoint_checks_shape(self) -> None:
        """Список обязан быть списком: иначе дальше идти не с чем."""
        with mock.patch.object(mf, "fetch_json", lambda *a: {"error": "nope"}):
            with self.assertRaises(mf.QueryFailed):
                mf.fetch_offer_list()


class CollectTest(unittest.TestCase):
    """Обход: фильтр, карточки, запись частями, сбой на одной не рушит прогон."""

    def setUp(self) -> None:
        patcher = mock.patch.object(mf.time, "sleep", lambda _s: None)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "raw_manfred.jsonl"

    def offers(self, count: int) -> list[dict]:
        """count дата-ролей плюс один посторонний оффер."""
        rows = [{**OFFER, "id": 100 + i, "position": f"Data Analyst {i}"}
                for i in range(count)]
        rows.append({**OFFER, "id": 999, "position": "Backend Engineer"})
        return rows

    def run_collect(self, offers, card_answer, limit=None):
        def fake_get(url, params=None, timeout=None):
            if url == mf.LIST_URL:
                return FakeResponse(200, offers)
            return card_answer(url)

        with mock.patch.object(mf.requests, "get", fake_get):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                rows, totals = mf.collect(limit=limit, path=self.path)
        return rows, totals, out.getvalue()

    def test_only_data_roles_fetched(self) -> None:
        """Посторонний оффер в карточку не идёт: 3 запроса вместо 4."""
        asked = []

        def card(url):
            asked.append(url)
            return FakeResponse(200, CARD)

        rows, totals, _ = self.run_collect(self.offers(3), card)
        self.assertEqual(len(rows), 3)
        self.assertEqual(totals["candidates"], 3)
        self.assertEqual(totals["offers_total"], 4)
        self.assertEqual(len(asked), 3)
        self.assertNotIn("999", " ".join(asked))

    def test_one_card_fails_others_continue(self) -> None:
        """Карточка с постоянной ошибкой пропускается, остальные собираются."""
        def card(url):
            return FakeResponse(404) if "/101" in url else FakeResponse(200, CARD)

        rows, totals, _ = self.run_collect(self.offers(3), card)
        self.assertEqual(len(rows), 2)
        self.assertEqual(list(totals["failed"]), ["101"])

    def test_saved_in_batches(self) -> None:
        """Пишем частями: при BATCH_SIZE=2 и 5 вакансиях будет три записи."""
        with mock.patch.object(mf, "BATCH_SIZE", 2):
            rows, _totals, out = self.run_collect(
                self.offers(5), lambda url: FakeResponse(200, CARD))
        self.assertEqual(out.count("записано"), 3)
        saved = [json.loads(line) for line in
                 self.path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(saved), 5)
        self.assertEqual(len(rows), 5)

    def test_partial_result_survives_late_failure(self) -> None:
        """Сбой на последней карточке не стоит собранного раньше."""
        def card(url):
            return FakeResponse(404) if "/104" in url else FakeResponse(200, CARD)

        with mock.patch.object(mf, "BATCH_SIZE", 2):
            rows, _totals, _ = self.run_collect(self.offers(5), card)
        saved = self.path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(saved), 4)
        self.assertEqual(len(rows), 4)

    def test_limit(self) -> None:
        rows, totals, _ = self.run_collect(
            self.offers(5), lambda url: FakeResponse(200, CARD), limit=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(totals["candidates"], 2)


class MeasureTest(unittest.TestCase):
    """Измерения, которые печатаются после прогона."""

    def measure_output(self, rows, totals=None) -> str:
        totals = totals or {"offers_total": 100, "candidates": len(rows), "failed": {}}
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            mf.measure(rows, totals)
        return buffer.getvalue()

    def test_empty(self) -> None:
        self.assertIn("измерять нечего", self.measure_output([]))

    def test_basic_numbers(self) -> None:
        rows = [mf.normalize(OFFER, CARD),
                mf.normalize({**OFFER, "id": 2, "salaryFrom": 0, "salaryTo": 0}, CARD)]
        out = self.measure_output(rows)
        self.assertIn("измерения по 2 собранным вакансиям", out)
        self.assertIn("зарплатная вилка указана: 1 из 2 (50%)", out)
        self.assertIn("уникальных по source_id: 2", out)

    def test_remote_share(self) -> None:
        rows = [mf.normalize(OFFER, CARD),
                mf.normalize({**OFFER, "id": 2, "remotePercentage": 0}, CARD)]
        out = self.measure_output(rows)
        self.assertIn("100% у 1 вакансий", out)
        self.assertIn("0% у 1", out)

    def test_warns_on_empty_description(self) -> None:
        rows = [mf.normalize(OFFER, {})]
        self.assertIn("ВНИМАНИЕ: без описания 1", self.measure_output(rows))

    def test_duplicates_counted(self) -> None:
        rows = [mf.normalize(OFFER, CARD), mf.normalize(OFFER, CARD)]
        self.assertIn("дублей внутри прогона: 1", self.measure_output(rows))


if __name__ == "__main__":
    unittest.main()
