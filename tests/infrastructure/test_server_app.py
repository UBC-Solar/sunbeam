import pytest

# Server modules need the broker extra (fastapi, docker). Skip cleanly in
# executor/worker environments where those are not installed.
pytest.importorskip("fastapi")
pytest.importorskip("docker")


class TestCreateApp:
    def test_app_builds_without_database_or_docker(self):
        from server.main import create_app

        app = create_app(lifespan=None)

        paths = set(app.openapi()["paths"])
        assert "/health" in paths
        assert "/workers/launch" in paths
        assert "/events" in paths
        assert "/pipeline-editions" in paths

    def test_importing_server_modules_creates_no_engine(self):
        import server.db

        assert server.db._engine is None
