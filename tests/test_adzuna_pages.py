"""Проверки fetch/adzuna_pages.py: что пишется в raw.vacancy_pages при разных ответах сайта.

Ни сеть, ни BigQuery тесты не вызывают. HTTP-клиент httpx настоящий, но его
транспорт подменён на httpx.MockTransport: ответ выбирается по source_id в
адресе страницы. Паузы подменены и по-настоящему не ждут.

Запрос кандидатов (SQL) здесь не проверяется — он подменён готовым списком.

Запуск из папки проекта:
    python -m unittest tests.test_adzuna_pages -v
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import unittest
from unittest import mock

import httpx

import fetch.adzuna_pages as fp

# Настоящий конструктор клиента: в тестах httpx.Client подменён обёрткой,
# которая вызывает его, добавив поддельный транспорт.
REAL_HTTPX_CLIENT = httpx.Client

# Параметры, с которыми API отдаёт ссылки на вакансии.
API_PARAMS = "utm_medium=api&utm_source=0615359f"

# Описание в JSON-LD приходит с HTML-разметкой — так и должно лечь в page_text.
DESCRIPTION = "<p>We are looking for a <strong>Data Analyst</strong>.</p><p>SQL, dbt, BigQuery.</p>"

# Видимое описание на странице — как в section.adp-body у Adzuna.
VISIBLE_DESCRIPTION = '<section class="adp-body">We are looking for a Data Analyst. SQL, dbt, BigQuery.</section>'
REMOVED_BANNER = '<div class="bg-blue-100">Lo sentimos, este empleo ya no está disponible</div>'


def job_posting_ld(data: dict | list | None = None) -> str:
    """Блок <script type="application/ld+json"> с разметкой вакансии."""
    if data is None:
        data = {"@context": "https://schema.org", "@type": "JobPosting",
                "title": "Data Analyst", "description": DESCRIPTION}
    return f'<script type="application/ld+json">{json.dumps(data)}</script>'


def page(body: str, head: str = "") -> str:
    return f"<!doctype html><html><head><title>Data Analyst</title>{head}</head><body>{body}</body></html>"


def details_link(source_id: str, host: str = "www.adzuna.es") -> str:
    """Ссылка на вакансию в том виде, в каком её сохраняет коллектор из API."""
    return f"https://{host}/details/{source_id}?{API_PARAMS}"


def make_candidate(source_id: str, url: str | None = None) -> dict:
    """Кандидат в том виде, в каком его возвращает fetch_candidates."""
    return {
        "vacancy_key": f"adzuna:{source_id}",
        "source_id": source_id,
        "url": url or details_link(source_id),
    }


class FetchAdzunaPagesTest(unittest.TestCase):

    def setUp(self) -> None:
        self.requests_by_id: dict[str, int] = {}   # source_id → сколько запросов ушло
        self.requested_urls: list[str] = []
        self.user_agents: set[str] = set()
        self.sleeps: list[float] = []
        self.saved_rows: list[dict] = []
        for patcher in [
            mock.patch.dict(os.environ, {"BQ_PROJECT": "fake-project"}),
            mock.patch.object(fp.httpx, "Client", self.client_with_mock_transport),
            mock.patch.object(fp.bigquery, "Client", lambda project: None),
            mock.patch.object(fp, "ensure_table", lambda bq, table_id: None),
            mock.patch.object(fp, "save", lambda bq, table_id, rows: self.saved_rows.extend(rows)),
            mock.patch.object(fp.time, "sleep", self.sleeps.append),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    # --- подмены ---

    def client_with_mock_transport(self, **kwargs) -> httpx.Client:
        """httpx.Client, у которого вместо сети — handle_request."""
        return REAL_HTTPX_CLIENT(transport=httpx.MockTransport(self.handle_request), **kwargs)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Отвечает по source_id из адреса /details/<source_id>."""
        self.requested_urls.append(str(request.url))
        self.user_agents.add(request.headers["User-Agent"])
        if "/land/ad/" in request.url.path:
            raise AssertionError("скрипт запросил /land/ad/, а robots.txt это запрещает")
        source_id = request.url.path.removeprefix("/details/")
        attempt = self.requests_by_id.get(source_id, 0)
        self.requests_by_id[source_id] = attempt + 1

        if source_id == "live":
            return httpx.Response(200, html=page(VISIBLE_DESCRIPTION, head=job_posting_ld()))
        if source_id == "live_graph":
            graph = {"@context": "https://schema.org", "@graph": [
                {"@type": "Organization", "name": "Adzuna"},
                {"@type": ["JobPosting"], "description": DESCRIPTION},
            ]}
            return httpx.Response(200, html=page(VISIBLE_DESCRIPTION, head=job_posting_ld(graph)))
        if source_id == "no_ld":
            return httpx.Response(200, html=page(VISIBLE_DESCRIPTION))
        if source_id == "removed":
            # Как у настоящей снятой вакансии: 404, плашка, описание видно, разметки нет.
            return httpx.Response(404, html=page(REMOVED_BANNER + VISIBLE_DESCRIPTION))
        if source_id == "removed_with_ld":
            # Строже настоящей: 404, но разметка JobPosting на странице осталась.
            return httpx.Response(404, html=page(REMOVED_BANNER + VISIBLE_DESCRIPTION, head=job_posting_ld()))
        if source_id == "redirect_removed":
            if "i" not in request.url.params:
                return httpx.Response(303, headers={"Location": f"/details/redirect_removed?i=abc&{API_PARAMS}"})
            return httpx.Response(404, html=page(REMOVED_BANNER + VISIBLE_DESCRIPTION))
        if source_id.startswith("blocked"):
            return httpx.Response(403, html="<html><body><h1>403 ERROR</h1>Request blocked.</body></html>")
        if source_id.startswith("timeout"):
            raise httpx.ReadTimeout("The read operation timed out", request=request)
        if source_id == "busy_once":
            if attempt == 0:
                return httpx.Response(503, html="<html><body>Service Unavailable</body></html>")
            return httpx.Response(200, html=page(VISIBLE_DESCRIPTION, head=job_posting_ld()))
        if source_id == "broken_ld":
            return httpx.Response(200, html=page(VISIBLE_DESCRIPTION,
                                                 head='<script type="application/ld+json">{"@type": "JobPosting",</script>'))
        raise AssertionError(f"неизвестный source_id {source_id}")

    def run_fetch(self, candidates: list[dict]) -> tuple[str, str | None]:
        """Прогоняет fetch_pages. Возвращает лог и сообщение SystemExit (None — код 0)."""
        log = io.StringIO()
        with mock.patch.object(fp, "fetch_candidates", lambda bq, project, limit: candidates), \
                contextlib.redirect_stdout(log):
            try:
                fp.fetch_pages(limit=fp.MAX_PER_RUN)
            except SystemExit as stop:
                return log.getvalue(), stop.code
        return log.getvalue(), None

    def row(self, source_id: str) -> dict:
        (found,) = [r for r in self.saved_rows if r["source_id"] == source_id]
        return found

    # --- живая страница ---

    def test_200_with_job_posting(self) -> None:
        """Живая страница: код 200, разметка есть, текст из JSON-LD как есть."""
        log, exit_message = self.run_fetch([make_candidate("live")])

        self.assertIsNone(exit_message, log)
        row = self.row("live")
        self.assertEqual(row["vacancy_key"], "adzuna:live")
        self.assertEqual(row["url_requested"], details_link("live"))
        self.assertEqual(row["http_status"], 200)
        self.assertEqual(row["final_url"], details_link("live"))
        self.assertIs(row["has_job_posting_ld"], True)
        self.assertEqual(row["page_text"], DESCRIPTION)
        self.assertEqual(row["text_length"], len(DESCRIPTION))
        self.assertIsNotNone(row["fetched_at"])
        self.assertIsNone(row["fetch_error"])
        # Запрос — с честной подписью и по сохранённой ссылке целиком, с параметрами.
        self.assertEqual(self.user_agents, {"vacancy-radar/1.0 (personal job search)"})
        self.assertEqual(self.requested_urls, [details_link("live")])

    def test_job_posting_inside_graph(self) -> None:
        """Разметка внутри @graph и @type списком тоже находится."""
        self.run_fetch([make_candidate("live_graph")])

        row = self.row("live_graph")
        self.assertIs(row["has_job_posting_ld"], True)
        self.assertEqual(row["page_text"], DESCRIPTION)

    def test_nl_link_requested_as_is(self) -> None:
        """Ссылка на .nl запрашивается как сохранена — с доменом и параметрами."""
        link = details_link("live", host="www.adzuna.nl")
        self.run_fetch([make_candidate("live", url=link)])

        self.assertEqual(self.requested_urls, [link])
        self.assertEqual(self.row("live")["url_requested"], link)

    # --- пропуск /land/ad/ ---

    def test_land_ad_skipped_without_request(self) -> None:
        """/land/ad/ не запрашиваем: строка с причиной, остальные вакансии идут дальше."""
        land_link = f"https://www.adzuna.es/land/ad/5870446791?se=abc&{API_PARAMS}&v=DEF"
        log, exit_message = self.run_fetch([make_candidate("5870446791", url=land_link),
                                            make_candidate("live")])

        self.assertEqual(self.requested_urls, [details_link("live")])
        row = self.row("5870446791")
        self.assertEqual(row["url_requested"], land_link)
        self.assertIsNone(row["http_status"])
        self.assertIsNone(row["final_url"])
        self.assertIsNone(row["has_job_posting_ld"])
        self.assertIsNone(row["page_text"])
        self.assertEqual(row["fetch_error"], fp.SKIP_REASON)
        self.assertIsNone(exit_message, log)
        self.assertIn("пропущено 1", log)
        self.assertIn("  пропуск: adzuna:5870446791 — ", log)

    def test_only_land_ad_exits_with_code_0(self) -> None:
        """Одни пропуски — запросов не было, аварии нет, паузы не нужны."""
        land_link = f"https://www.adzuna.es/land/ad/1?se=abc&{API_PARAMS}"
        log, exit_message = self.run_fetch([make_candidate("1", url=land_link)])

        self.assertEqual(self.requested_urls, [])
        self.assertEqual(self.sleeps, [])
        self.assertIsNone(exit_message, log)

    # --- снятая вакансия ---

    def test_404_with_description_on_page(self) -> None:
        """Снятая вакансия: 404, описание на странице есть, но page_text пустой."""
        log, exit_message = self.run_fetch([make_candidate("removed"), make_candidate("removed_with_ld")])

        self.assertIsNone(exit_message, log)
        for source_id, has_ld in [("removed", False), ("removed_with_ld", True)]:
            with self.subTest(source_id=source_id):
                row = self.row(source_id)
                self.assertEqual(row["http_status"], 404)
                # Признак разметки пишется при любом коде.
                self.assertIs(row["has_job_posting_ld"], has_ld)
                self.assertIsNone(row["page_text"])
                self.assertIsNone(row["text_length"])
                # 404 — штатный ответ сайта, а не сбой; повторов не было.
                self.assertIsNone(row["fetch_error"])
                self.assertEqual(self.requests_by_id[source_id], 1)

    def test_redirect_to_404(self) -> None:
        """Редирект на снятую вакансию: пишется конечный адрес и его код."""
        self.run_fetch([make_candidate("redirect_removed")])

        row = self.row("redirect_removed")
        self.assertEqual(row["url_requested"], details_link("redirect_removed"))
        self.assertEqual(row["final_url"], f"https://www.adzuna.es/details/redirect_removed?i=abc&{API_PARAMS}")
        self.assertEqual(row["http_status"], 404)
        self.assertIsNone(row["page_text"])

    # --- 403 и код возврата ---

    def test_403(self) -> None:
        """403: код записан, текста нет, не повторяем и сбоем не считаем."""
        log, exit_message = self.run_fetch([make_candidate("blocked_1"), make_candidate("live")])

        row = self.row("blocked_1")
        self.assertEqual(row["http_status"], 403)
        self.assertIs(row["has_job_posting_ld"], False)
        self.assertIsNone(row["page_text"])
        self.assertIsNone(row["fetch_error"])
        self.assertEqual(self.requests_by_id["blocked_1"], 1)
        # Одна страница скачалась — код 0.
        self.assertIsNone(exit_message, log)

    def test_all_403_is_an_outage(self) -> None:
        """Все запросы получили 403 — авария, код 1; строки записаны, итог напечатан."""
        log, exit_message = self.run_fetch([make_candidate("blocked_1"), make_candidate("blocked_2")])

        self.assertIsInstance(exit_message, str, log)
        self.assertIn("все 2 запросов получили 403", exit_message)
        self.assertEqual(len(self.saved_rows), 2)
        self.assertIn("итог: вакансий 2: запрошено 2, скачано 0", log)

    def test_nothing_downloaded_exits_with_code_1(self) -> None:
        """403 вперемешку с таймаутом и ни одной скачанной страницы — тоже код 1."""
        log, exit_message = self.run_fetch([make_candidate("blocked_1"), make_candidate("timeout_1")])

        self.assertIsInstance(exit_message, str, log)
        self.assertIn("Не скачалась ни одна страница из 2", exit_message)

    # --- сбои ---

    def test_timeout(self) -> None:
        """Таймаут: повторы, строка со сбоем, остальные вакансии обрабатываются."""
        log, exit_message = self.run_fetch([make_candidate("timeout_1"), make_candidate("live")])

        self.assertEqual(self.requests_by_id["timeout_1"], len(fp.RETRY_PAUSES) + 1)
        retry_pauses = [seconds for seconds in self.sleeps if seconds != fp.PAUSE_SECONDS]
        self.assertEqual(retry_pauses, fp.RETRY_PAUSES)
        row = self.row("timeout_1")
        self.assertIsNone(row["http_status"])
        self.assertIsNone(row["final_url"])
        # Страницы нет — наличие разметки неизвестно, а не «нет».
        self.assertIsNone(row["has_job_posting_ld"])
        self.assertIsNone(row["page_text"])
        self.assertIn("ReadTimeout", row["fetch_error"])
        # Следующая вакансия обработана, прогон успешен.
        self.assertEqual(self.row("live")["http_status"], 200)
        self.assertIsNone(exit_message, log)
        self.assertIn("итог: вакансий 2: запрошено 2, скачано 1, с текстом 1, пропущено 0, сбоев 1", log)
        self.assertIn("  сбой: adzuna:timeout_1 — ", log)

    def test_200_without_json_ld(self) -> None:
        """Код 200, но разметки нет: признак false, текст не пишем."""
        self.run_fetch([make_candidate("no_ld")])

        row = self.row("no_ld")
        self.assertEqual(row["http_status"], 200)
        self.assertIs(row["has_job_posting_ld"], False)
        self.assertIsNone(row["page_text"])
        self.assertIsNone(row["text_length"])
        self.assertIsNone(row["fetch_error"])

    def test_temporary_503_is_retried(self) -> None:
        """503 повторяется; после успешного повтора строка как у живой страницы."""
        self.run_fetch([make_candidate("busy_once")])

        self.assertEqual(self.requests_by_id["busy_once"], 2)
        self.assertEqual([s for s in self.sleeps if s != fp.PAUSE_SECONDS], fp.RETRY_PAUSES[:1])
        row = self.row("busy_once")
        self.assertEqual(row["http_status"], 200)
        self.assertEqual(row["page_text"], DESCRIPTION)
        self.assertIsNone(row["fetch_error"])

    def test_broken_json_ld(self) -> None:
        """Неразборчивый JSON-LD: наличие разметки неизвестно, причина в fetch_error."""
        log, exit_message = self.run_fetch([make_candidate("broken_ld")])

        row = self.row("broken_ld")
        self.assertEqual(row["http_status"], 200)
        self.assertIsNone(row["has_job_posting_ld"])
        self.assertIsNone(row["page_text"])
        self.assertIn("JSON-LD не разбирается", row["fetch_error"])
        # Страница всё же скачалась — аварии нет.
        self.assertIsNone(exit_message, log)


if __name__ == "__main__":
    unittest.main()
