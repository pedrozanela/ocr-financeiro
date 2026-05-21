"""Cliente HTTP para API Techfin (PARC) com OAuth2 password grant + cache de token.

Secrets armazenados no scope 'techfin' do Databricks:
  - parc_client_id
  - parc_client_secret
  - parc_oauth_user
  - parc_oauth_password

No app (FastAPI em Databricks Apps), usa WorkspaceClient para ler secrets.
Em dev local, faz fallback via `databricks secrets get-secret` CLI.
"""
import json
import logging
import os
import subprocess
import time
from typing import Optional

import requests

logger = logging.getLogger("techfin")


class TechfinClient:
    OAUTH_URL = "https://parc.supplierapi.com.br/oauth2/access-token"
    BALANCO_URL = "https://parc.supplierapi.com.br/databricks/v1/balanco"
    # Scope configuravel via env TECHFIN_SECRET_SCOPE (default: 'techfin-ocr')
    SCOPE = os.environ.get("TECHFIN_SECRET_SCOPE", "techfin-ocr")
    KEYS = ("parc_client_id", "parc_client_secret", "parc_oauth_user", "parc_oauth_password")
    TOKEN_REFRESH_MARGIN = 60  # renova 60s antes de expirar

    def __init__(self):
        secrets = self._load_secrets()
        self.client_id = secrets["parc_client_id"]
        self.client_secret = secrets["parc_client_secret"]
        self.oauth_user = secrets["parc_oauth_user"]
        self.oauth_password = secrets["parc_oauth_password"]
        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0

    def _load_secrets(self) -> dict:
        is_app = bool(os.environ.get("DATABRICKS_APP_NAME"))
        if is_app:
            return self._load_secrets_via_sdk()
        return self._load_secrets_via_cli()

    def _load_secrets_via_sdk(self) -> dict:
        from databricks.sdk import WorkspaceClient
        import base64
        w = WorkspaceClient()
        out = {}
        for k in self.KEYS:
            r = w.api_client.do(
                "GET",
                f"/api/2.0/secrets/get",
                query={"scope": self.SCOPE, "key": k},
            )
            # Resposta vem com value base64
            val = r.get("value", "")
            try:
                out[k] = base64.b64decode(val).decode("utf-8")
            except Exception:
                out[k] = val
        return out

    def _load_secrets_via_cli(self) -> dict:
        profile = os.environ.get("DATABRICKS_PROFILE", "fe-vm-fevm-pzanela-classic-aws")
        out = {}
        for k in self.KEYS:
            r = subprocess.run(
                ["databricks", "secrets", "get-secret", self.SCOPE, k, "-p", profile, "-o", "json"],
                capture_output=True, text=True,
            )
            if r.returncode != 0:
                raise RuntimeError(f"Falha lendo secret {self.SCOPE}/{k}: {r.stderr}")
            data = json.loads(r.stdout)
            # CLI retorna {"key": "...", "value": "<base64>"}
            import base64
            out[k] = base64.b64decode(data["value"]).decode("utf-8")
        return out

    def _get_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expires_at - self.TOKEN_REFRESH_MARGIN:
            return self._token

        logger.info("Techfin: obtendo novo OAuth token")
        response = requests.post(
            self.OAUTH_URL,
            auth=(self.client_id, self.client_secret),
            headers={"Content-Type": "application/json"},
            json={
                "grant_type": "password",
                "username": self.oauth_user,
                "password": self.oauth_password,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        self._token = data["access_token"]
        ttl = int(data.get("expires_in", 3600))
        self._token_expires_at = now + ttl
        logger.info("Techfin: token obtido, expira em %ds", ttl)
        return self._token

    def submit_balanco(self, payload: dict, timeout: int = 60) -> dict:
        """Envia payload pra API Techfin.

        Returns: dict da resposta (JSON), ou {} se body vazio.
        Raises:
          requests.HTTPError em 4xx/5xx (exceto 409 — chamador decide tratamento).
          requests.Timeout se exceder o timeout.
        """
        token = self._get_token()
        response = requests.post(
            self.BALANCO_URL,
            headers={
                "client_id": self.client_id,
                "access_token": token,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )
        # NÃO chama raise_for_status aqui — chamador trata 409 como upsert idempotente
        response.raise_for_status()
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError:
            return {"raw": response.text}
