"""spark-submit carga el conector y la configuración del clúster antes de pytest."""
import pytest

raise SystemExit(pytest.main(["-q", "/app/tests/test_spark.py"]))
