"""Database layer — Lakebase (PostgreSQL) via psycopg2."""
import os
import json
import subprocess
import time
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Connection config — read from env vars set by app.yaml / databricks.yml
# ---------------------------------------------------------------------------
LAKEBASE_HOST = os.environ.get("LAKEBASE_HOST", "")
LAKEBASE_DB = os.environ.get("LAKEBASE_DB", "ocr_financeiro")
LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "ocr-financeiro")
LAKEBASE_BRANCH = os.environ.get("LAKEBASE_BRANCH", "production")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "primary")
IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

_cached_conn = None
_token_cache = {"token": "", "email": "", "expires": 0}


def _get_token() -> tuple[str, str]:
    """Get OAuth token and email for Lakebase connection."""
    now = time.time()
    if _token_cache["token"] and _token_cache["expires"] > now:
        return _token_cache["token"], _token_cache["email"]

    if IS_DATABRICKS_APP:
        from databricks.sdk import WorkspaceClient
        import logging
        logger = logging.getLogger("lakebase")
        w = WorkspaceClient()
        endpoint_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/{LAKEBASE_ENDPOINT}"

        # Generate Lakebase-specific database credential
        import base64
        try:
            resp = w.api_client.do(
                "POST",
                "/api/2.0/postgres/credentials",
                body={"endpoint": endpoint_path},
            )
            token = resp.get("token", "")
            # Decode JWT to check sub claim
            parts = token.split(".")
            if len(parts) >= 2:
                payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
                claims = json.loads(base64.urlsafe_b64decode(payload))
                logger.warning(f"Token claims: sub={claims.get('sub')}, client_id={claims.get('client_id')}, aud={claims.get('aud')}")
            logger.warning(f"Got Lakebase credential token (len={len(token)})")
        except Exception as e:
            logger.warning(f"Failed to get Lakebase credential: {e}")
            token = ""

        try:
            me = w.current_user.me()
            email = me.user_name
            logger.warning(f"SP user: {email}")
        except Exception as e:
            logger.warning(f"Failed to get SP identity: {e}")
            email = os.environ.get("DATABRICKS_APP_SP_ID", "app-sp")
    else:
        # Local dev: use CLI to generate database credential
        profile = os.environ.get("DATABRICKS_PROFILE", "fe-vm-fevm-pzanela-classic-aws")
        endpoint_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/{LAKEBASE_ENDPOINT}"
        r = subprocess.run(
            ["databricks", "postgres", "generate-database-credential", endpoint_path, "-p", profile, "-o", "json"],
            capture_output=True, text=True,
        )
        token = json.loads(r.stdout)["token"]
        r2 = subprocess.run(
            ["databricks", "current-user", "me", "-p", profile, "-o", "json"],
            capture_output=True, text=True,
        )
        email = json.loads(r2.stdout)["userName"]

    _token_cache["token"] = token
    _token_cache["email"] = email
    _token_cache["expires"] = now + 2400  # cache for 40 min (tokens last 1h)
    return token, email


def get_connection():
    """Get or create a psycopg2 connection to Lakebase."""
    global _cached_conn
    if _cached_conn and not _cached_conn.closed:
        try:
            with _cached_conn.cursor() as cur:
                cur.execute("SELECT 1")
            return _cached_conn
        except Exception:
            try:
                _cached_conn.close()
            except Exception:
                pass
            _cached_conn = None

    token, email = _get_token()
    _cached_conn = psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname=LAKEBASE_DB,
        user=email,
        password=token,
        sslmode="require",
        connect_timeout=10,
    )
    _cached_conn.autocommit = True
    return _cached_conn


def execute_sql(statement: str, parameters: list | None = None, **_kwargs) -> list[dict]:
    """Execute SQL and return rows as dicts.

    Parameters use the Databricks named-parameter format for compatibility:
    [{"name": "foo", "value": "bar"}, ...] → converted to %(foo)s style.
    """
    conn = get_connection()
    # Convert Databricks-style named params to psycopg2 %(name)s style
    pg_params = {}
    pg_statement = statement
    if parameters:
        for p in parameters:
            pg_params[p["name"]] = p["value"]
        # Replace :name with %(name)s
        for p in parameters:
            pg_statement = pg_statement.replace(f":{p['name']}", f"%({p['name']})s")

    # Replace Databricks SQL functions with PostgreSQL equivalents
    import re
    pg_statement = pg_statement.replace("CURRENT_TIMESTAMP()", "NOW()")
    # CAST(expr AS STRING) → (expr)::text  (handles any expression inside)
    pg_statement = re.sub(r'CAST\((.+?) AS STRING\)', r'(\1)::text', pg_statement)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(pg_statement, pg_params or None)
        if cur.description:
            rows = cur.fetchall()
            # Serialize Decimal/datetime to JSON-safe types
            from decimal import Decimal
            from datetime import datetime, date
            result = []
            for row in rows:
                d = {}
                for k, v in dict(row).items():
                    if isinstance(v, Decimal):
                        d[k] = float(v)
                    elif isinstance(v, (datetime, date)):
                        d[k] = v.isoformat()
                    else:
                        d[k] = v
                result.append(d)
            return result
        return []


def execute_update(statement: str, parameters: list | None = None) -> None:
    """Execute an INSERT/UPDATE/DELETE statement."""
    execute_sql(statement, parameters)
