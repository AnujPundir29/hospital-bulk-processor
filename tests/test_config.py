from app.config import Settings


def test_settings_from_env_casts_types(monkeypatch):
    monkeypatch.setenv("MAX_CONCURRENCY", "7")
    monkeypatch.setenv("UPSTREAM_TIMEOUT_SECONDS", "1.5")
    monkeypatch.setenv("UPSTREAM_BASE_URL", "http://upstream.test")
    monkeypatch.setenv("MAX_CSV_ROWS", "")  # empty → default
    s = Settings.from_env()
    assert (s.max_concurrency, s.upstream_timeout_seconds, s.upstream_base_url, s.max_csv_rows) == (7, 1.5, "http://upstream.test", 20)
