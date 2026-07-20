"""Keep pytest fixtures isolated from the live trading vault and snapshot."""

import os
import tempfile
from pathlib import Path


_TEST_ROOT = Path(tempfile.mkdtemp(prefix="binance-autotrend-tests-"))
os.environ.setdefault("HERMES_DATA_DIR", str(_TEST_ROOT))
os.environ.setdefault("HERMES_ENV_PATH", str(_TEST_ROOT / ".env"))
Path(os.environ["HERMES_ENV_PATH"]).touch()
# Prevent load_dotenv() from making unit tests call the live Binance account.
os.environ["BINANCE_API_KEY"] = ""
os.environ["BINANCE_API_SECRET"] = ""
