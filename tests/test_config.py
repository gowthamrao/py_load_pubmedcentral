from __future__ import annotations


def test_settings_load_from_env(monkeypatch):
    """
    Tests that the Settings object correctly loads values from environment variables.
    """
    # Set mock environment variables
    monkeypatch.setenv("PMC_DB_USER", "test_user")
    monkeypatch.setenv("PMC_DB_PASSWORD", "test_password")
    monkeypatch.setenv("PMC_DB_HOST", "db.example.com")
    monkeypatch.setenv("PMC_DB_PORT", "1234")
    monkeypatch.setenv("PMC_DB_NAME", "test_db")

    # We need to re-import the settings module to trigger the re-evaluation
    # of the settings object with the new environment variables.
    # A cleaner way in a larger app might be to use a factory function.
    from py_load_pubmedcentral import config
    import importlib
    importlib.reload(config)

    # Assert that the settings object has the correct values
    assert config.settings.db_user == "test_user"
    assert config.settings.db_password == "test_password"
    assert config.settings.db_host == "db.example.com"
    assert config.settings.db_port == 1234
    assert config.settings.db_name == "test_db"
