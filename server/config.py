"""Re-export from root config — server routes use `from ..config import X`."""
from config import *  # noqa: F401,F403
from config import get_client  # noqa: F401 — explicit re-export
