import time
from databricks.sdk.service.sql import StatementState
from .config import get_client, WAREHOUSE_ID


def execute_sql(statement: str, parameters: list | None = None) -> list[dict]:
    """Execute SQL via Databricks Statement Execution API and return rows as dicts."""
    client = get_client()

    # Build named parameters if provided
    named_params = None
    if parameters:
        from databricks.sdk.service.sql import StatementParameterListItem
        named_params = [
            StatementParameterListItem(name=p["name"], value=str(p["value"]), type=p.get("type", "STRING"))
            for p in parameters
        ]

    response = client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        parameters=named_params,
        wait_timeout="30s",
    )

    # Poll until done
    while response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(0.5)
        response = client.statement_execution.get_statement(response.statement_id)

    if response.status.state != StatementState.SUCCEEDED:
        err = response.status.error
        raise RuntimeError(f"SQL failed [{response.status.state}]: {err.message if err else 'unknown'}")

    result = response.result
    if not result or not result.data_array:
        return []

    cols = [c.name for c in response.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in result.data_array]


def execute_update(statement: str, parameters: list | None = None) -> None:
    """Execute an INSERT/UPDATE/DELETE statement."""
    execute_sql(statement, parameters)
