"""Проверки collectors/careerjet.py: параметры запроса, нормализация, поведение при сбоях.

Сеть не вызывается: requests.get подменён заглушкой, которая отдаёт заранее
заготовленные ответы. Паузы подменены и по-настоящему не ждут.

Запуск из папки проекта:
    python -m unittest tests.test_careerjet -v
"""

from __future__ import annotations

import contextlib
import io
import unittest
from unittest import mock

import requests

import collectors.careerjet as cj

# Вакансия в том виде, в каком её отдаёт v4. Поля ровно те, что в документации:
# id среди них нет — отсюда и make_source_id.
JOB = {
    "title": "Data Analyst",
    "company": "Acme Data",
    "locations": "Barcelona",
    "date": "Wed, 12 Aug 2026 07:55:03 GMT",
    "description": "Buscamos un <b>Data</b> <b>Analyst</b>. SQL, dbt, BigQuery.",
    "url": "https://jobviewtrack.com/v2/abc123",
    "salary": "&euro;33000 - 36000 per year",
    "salary_min": "33000",
    "salary_max": "36000",
    "salary_currency_code": "EUR",
    "salary_type": "Y",
}


class FakeResponse:
    """Минимальный двойник requests.Response: коллектору нужны только эти два."""

    def __init__(self, status_code: int, body: dict | None = None) -> None:
        self.status_code = status_code
        self._body = body or {}

    def json(self) -> dict:
        return self._body


def ok(jobs: list[dict] | None = None, hits: int = 1) -> FakeResponse:
    """Удачный ответ v4."""
    return FakeResponse(200, {"type": "JOBS", "hits": hits,
                              "jobs": JOB if jobs is None else jobs})


class BuildParamsTest(unittest.TestCase):
    """Параметры запроса. Главное здесь — fragment_size."""

    def test_fragment_size_is_sent(self) -> None:
        """Без fragment_size описание обрезано на ~250 символах — параметр обязателен."""
        params = cj.build_params("data analyst", "Barcelona")
        self.assertEqual(params["fragment_size"], 20000)

    def test_search_parameters(self) -> None:
        """Локаль испанская, фраза передана как есть, страница максимальная, сортировка по дате."""
        params = cj.build_params("data analyst", "Barcelona")
        self.assertEqual(params["locale_code"], "es_ES")
        self.assertEqual(params["keywords"], "data analyst")
        self.assertEqual(params["location"], "Barcelona")
        self.assertEqual(params["page_size"], 100)
        self.assertEqual(params["sort"], "date")

    def test_location_omitted_for_whole_country(self) -> None:
        """Поиск по всей Испании: параметр location не передаётся вовсе, а не пустой строкой."""
        params = cj.build_params("data analyst", None)
        self.assertNotIn("location", params)

    def test_required_user_fields(self) -> None:
        """user_ip и user_agent обязательны у v4; ip — из документационного диапазона."""
        params = cj.build_params("data analyst", None)
        self.assertTrue(params["user_ip"].startswith("203.0.113."))
        self.assertIn("vacancy-radar", params["user_agent"])


class NormalizeTest(unittest.TestCase):
    """Приведение к общей схеме."""

    def test_schema_matches_other_collectors(self) -> None:
        """Набор и ПОРЯДОК полей как у остальных: union all в staging сопоставляет по порядку."""
        row = cj.normalize(JOB, "data analyst / Barcelona")
        self.assertEqual(list(row), [
            "source", "source_id", "title", "company_name", "location",
            "remote", "url", "tags", "job_types", "description",
            "created_at_unix", "ingested_at", "source_page",
            "salary_min", "salary_max", "salary_text",
            "salary_period", "salary_currency", "query",
        ])

    def test_fields(self) -> None:
        row = cj.normalize(JOB, "data analyst / Barcelona")
        self.assertEqual(row["source"], "careerjet")
        self.assertEqual(row["title"], "Data Analyst")
        self.assertEqual(row["company_name"], "Acme Data")
        self.assertEqual(row["location"], "Barcelona")
        self.assertEqual(row["url"], "https://jobviewtrack.com/v2/abc123")
        self.assertEqual(row["query"], "data analyst / Barcelona")
        # Источник не сообщает, удалённая ли вакансия: None, а не False.
        self.assertIsNone(row["remote"])
        self.assertEqual(row["tags"], [])
        self.assertEqual(row["job_types"], [])

    def test_description_kept_with_markup(self) -> None:
        """raw хранит то, что пришло: <b> вокруг найденных слов не чистим."""
        row = cj.normalize(JOB, "q")
        self.assertIn("<b>Data</b>", row["description"])

    def test_salary_is_not_nulled(self) -> None:
        """У Careerjet нет признака предсказанной зарплаты — цифры пишем как есть.

        Это отличие от Adzuna, где при salary_is_predicted="1" мы пишем null.
        salary_type здесь — период (Y/M/W/D/H), а не флаг прогноза.
        """
        row = cj.normalize(JOB, "q")
        self.assertEqual(row["salary_min"], "33000")
        self.assertEqual(row["salary_max"], "36000")
        self.assertEqual(row["salary_period"], "Y")
        self.assertEqual(row["salary_currency"], "EUR")
        self.assertEqual(row["salary_text"], "&euro;33000 - 36000 per year")

    def test_missing_salary(self) -> None:
        """Вакансия без зарплаты: null, а не пустая строка."""
        row = cj.normalize({**JOB, "salary": "", "salary_min": None,
                            "salary_max": None, "salary_type": None}, "q")
        self.assertIsNone(row["salary_min"])
        self.assertIsNone(row["salary_text"])

    def test_date_to_unix(self) -> None:
        """RFC 2822 с зоной GMT переводится в unix-время."""
        self.assertEqual(cj.to_unix("Wed, 12 Aug 2026 07:55:03 GMT"), 1786521303)

    def test_date_missing(self) -> None:
        self.assertIsNone(cj.to_unix(None))
        self.assertIsNone(cj.to_unix(""))

    def test_date_without_timezone_treated_as_utc(self) -> None:
        """Без зоны считаем UTC, а не по часам машины: иначе данные уедут на два часа."""
        self.assertEqual(cj.to_unix("Wed, 12 Aug 2026 07:55:03"), 1786521303)


class SourceIdTest(unittest.TestCase):
    """Свой id: у Careerjet его нет."""

    def test_same_job_same_id(self) -> None:
        """Та же вакансия по другому запросу — тот же id, иначе дубли не склеятся."""
        self.assertEqual(cj.make_source_id(JOB), cj.make_source_id(dict(JOB)))

    def test_date_does_not_affect_id(self) -> None:
        """Агрегатор переставляет дату при переиндексации — id от этого меняться не должен."""
        moved = {**JOB, "date": "Thu, 13 Aug 2026 09:00:00 GMT"}
        self.assertEqual(cj.make_source_id(JOB), cj.make_source_id(moved))

    def test_different_company_different_id(self) -> None:
        other = {**JOB, "company": "Other Corp"}
        self.assertNotEqual(cj.make_source_id(JOB), cj.make_source_id(other))

    def test_empty_fields_do_not_collide(self) -> None:
        """Вакансия без компании и вакансия без города не должны слиться в одну."""
        no_company = {"title": "Data Analyst", "company": "", "locations": "Barcelona"}
        no_city = {"title": "Data Analyst", "company": "Barcelona", "locations": ""}
        self.assertNotEqual(cj.make_source_id(no_company), cj.make_source_id(no_city))


class FetchOneTest(unittest.TestCase):
    """Один запрос: повторы при временных сбоях, отказ при постоянных."""

    def setUp(self) -> None:
        self.sleeps: list[float] = []
        patcher = mock.patch.object(cj.time, "sleep", self.sleeps.append)
        patcher.start()
        self.addCleanup(patcher.stop)

    def run_with(self, responses: list) -> list[dict]:
        """Прогоняет fetch_one на заготовленной последовательности ответов."""
        self.calls: list[dict] = []

        def fake_get(url, params=None, auth=None, timeout=None):
            self.calls.append({"url": url, "params": params, "auth": auth})
            answer = responses[len(self.calls) - 1]
            if isinstance(answer, Exception):
                raise answer
            return answer

        with mock.patch.object(cj.requests, "get", fake_get):
            with contextlib.redirect_stdout(io.StringIO()):
                return cj.fetch_one("data analyst", "Barcelona", "secret-key")

    def test_success(self) -> None:
        jobs = self.run_with([ok([JOB])])
        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(self.calls), 1)

    def test_key_goes_to_basic_auth_not_url(self) -> None:
        """Ключ уходит HTTP Basic и в параметрах запроса не появляется."""
        self.run_with([ok([JOB])])
        self.assertEqual(self.calls[0]["auth"], ("secret-key", ""))
        self.assertNotIn("secret-key", str(self.calls[0]["params"]))
        self.assertNotIn("secret-key", self.calls[0]["url"])

    def test_temporary_error_retried(self) -> None:
        """503, потом 200: повтор сработал, пауза выдержана."""
        jobs = self.run_with([FakeResponse(503), ok([JOB])])
        self.assertEqual(len(jobs), 1)
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.sleeps, [5])

    def test_timeout_retried(self) -> None:
        jobs = self.run_with([requests.Timeout(), ok([JOB])])
        self.assertEqual(len(jobs), 1)
        self.assertEqual(self.sleeps, [5])

    def test_permanent_error_not_retried(self) -> None:
        """401 повтором не чинится: не тратим попытки."""
        with self.assertRaises(cj.QueryFailed) as caught:
            self.run_with([FakeResponse(401)])
        self.assertIn("401", str(caught.exception))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.sleeps, [])

    def test_retries_run_out(self) -> None:
        """Четыре попытки — и сдаёмся, а не ходим по кругу."""
        with self.assertRaises(cj.QueryFailed):
            self.run_with([FakeResponse(503)] * 4)
        self.assertEqual(len(self.calls), 4)
        self.assertEqual(self.sleeps, [5, 15, 45])

    def test_error_body_with_code_200(self) -> None:
        """Код 200, а в теле ошибка: это сбой запроса, а не «ничего не нашлось»."""
        body = {"type": "ERROR", "error": "Invalid locale_code"}
        with self.assertRaises(cj.QueryFailed) as caught:
            self.run_with([FakeResponse(200, body)])
        self.assertIn("Invalid locale_code", str(caught.exception))

    def test_connection_error_not_retried(self) -> None:
        with self.assertRaises(cj.QueryFailed):
            self.run_with([requests.ConnectionError("DNS")])
        self.assertEqual(self.sleeps, [])


class CollectTest(unittest.TestCase):
    """Обход всех запросов: сбой на одном не отменяет остальные."""

    def setUp(self) -> None:
        for patcher in [
            mock.patch.object(cj.time, "sleep", lambda _s: None),
            mock.patch.dict("os.environ", {"CAREERJET_API_KEY": "secret-key"}),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_one_query_fails_others_continue(self) -> None:
        """Один запрос упал навсегда — остальные одиннадцать всё равно собраны."""
        def fake_get(url, params=None, auth=None, timeout=None):
            if params["keywords"] == "data analyst" and params.get("location") == "Madrid":
                return FakeResponse(400)
            return ok([JOB])

        with mock.patch.object(cj.requests, "get", fake_get):
            with contextlib.redirect_stdout(io.StringIO()):
                rows, counts, failed = cj.collect()

        # 3 места × 4 фразы = 12 запросов, один упал.
        self.assertEqual(len(counts), 11)
        self.assertEqual(len(rows), 11)
        self.assertEqual(list(failed), ["data analyst / Madrid"])

    def test_all_queries_fail(self) -> None:
        """Не удалось ничего: counts пуст — __main__ по нему выходит с кодом 1."""
        with mock.patch.object(cj.requests, "get", lambda *a, **k: FakeResponse(400)):
            with contextlib.redirect_stdout(io.StringIO()):
                rows, counts, failed = cj.collect()
        self.assertEqual(rows, [])
        self.assertEqual(counts, {})
        self.assertEqual(len(failed), 12)

    def test_query_label_in_every_row(self) -> None:
        """В каждой строке видно, по какому запросу она приехала."""
        with mock.patch.object(cj.requests, "get", lambda *a, **k: ok([JOB])):
            with contextlib.redirect_stdout(io.StringIO()):
                rows, _counts, _failed = cj.collect()
        self.assertIn("data analyst / вся Испания", {row["query"] for row in rows})


class MeasureTest(unittest.TestCase):
    """Измерения, которые печатаются после каждого прогона."""

    def measure_output(self, rows: list[dict]) -> str:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            cj.measure(rows)
        return buffer.getvalue()

    def test_counts_and_median(self) -> None:
        rows = [cj.normalize(JOB, "q"),
                cj.normalize({**JOB, "description": "x" * 4000}, "q")]
        output = self.measure_output(rows)
        self.assertIn("измерения по 2 собранным вакансиям", output)
        self.assertIn("максимум 4000", output)

    def test_markup_not_counted_as_text(self) -> None:
        """<b> в длину описания не входит: считаем текст, а не разметку."""
        self.assertEqual(cj.text_length("<b>abc</b>"), 3)

    def test_truncation_warning(self) -> None:
        """Больше половины описаний с многоточием — предупреждение про fragment_size."""
        cut = [cj.normalize({**JOB, "description": "текст обрезан..."}, "q")] * 3
        output = self.measure_output(cut + [cj.normalize(JOB, "q")])
        self.assertIn("заканчивается многоточием: 3 из 4", output)
        self.assertIn("проверьте, работает ли fragment_size", output)

    def test_no_warning_when_full(self) -> None:
        output = self.measure_output([cj.normalize(JOB, "q")])
        self.assertNotIn("ВНИМАНИЕ", output)

    def test_data_role_share(self) -> None:
        """Доля дата-ролей считается той же регуляркой, что в витрине."""
        rows = [
            cj.normalize({**JOB, "title": "Data Analyst"}, "q"),
            cj.normalize({**JOB, "title": "Analytics Engineer"}, "q"),
            cj.normalize({**JOB, "title": "Business Intelligence Manager"}, "q"),
            cj.normalize({**JOB, "title": "Camarero"}, "q"),
        ]
        self.assertIn("роль про данные: 3 из 4", self.measure_output(rows))

    def test_word_boundaries(self) -> None:
        """Границы слов на месте: «big data engineering» — не data engineer."""
        rows = [cj.normalize({**JOB, "title": "Big Data Engineering Manager"}, "q")]
        self.assertIn("роль про данные: 0 из 1", self.measure_output(rows))

    def test_duplicates_counted(self) -> None:
        """Одна вакансия, найденная двумя запросами, — это дубль внутри прогона."""
        rows = [cj.normalize(JOB, "data analyst / Barcelona"),
                cj.normalize(JOB, "data analyst / вся Испания"),
                cj.normalize({**JOB, "company": "Other"}, "q")]
        output = self.measure_output(rows)
        self.assertIn("уникальных по source_id: 2, дублей внутри прогона: 1", output)

    def test_empty(self) -> None:
        """Пустой прогон не должен падать делением на ноль."""
        self.assertIn("измерять нечего", self.measure_output([]))


if __name__ == "__main__":
    unittest.main()
