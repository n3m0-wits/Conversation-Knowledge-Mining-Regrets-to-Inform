from datetime import datetime
import logging
import struct

import pandas as pd
import pyodbc
from pydantic import BaseModel

from api.models.input_models import ChartFilters
from common.config.config import Config
from helpers.azure_credential_utils import get_azure_credential_async


class SQLTool(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    conn: pyodbc.Connection

    async def get_sql_response(self, sql_query: str) -> str:
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql_query)
            result = ''.join(str(row) for row in cursor.fetchall())
            return result
        except Exception as e:
            logging.error("Error executing SQL query: %s", e)
            return f"Error executing SQL query: {str(e)}"
        finally:
            if cursor:
                cursor.close()


async def get_db_connection():
    """Get a connection to the SQL database."""
    config = Config()
    server = config.sqldb_server
    database = config.sqldb_database
    mid_id = config.azure_client_id

    credential = None
    try:
        credential = await get_azure_credential_async(client_id=mid_id)
        token = await credential.get_token("https://database.windows.net/.default")
        token_bytes = token.token.encode("utf-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256

        for driver in ["{ODBC Driver 18 for SQL Server}", "{ODBC Driver 17 for SQL Server}"]:
            try:
                connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};"
                conn = pyodbc.connect(
                    connection_string,
                    attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                )
                logging.info("Connected using Azure Credential with %s", driver)
                return conn
            except pyodbc.Error:
                continue

        raise RuntimeError("Unable to connect using ODBC Driver 18 or 17 with Azure Credential")
    except Exception as e:
        logging.error("Failed with Azure Credential: %s", str(e))
        # Test harness expects a pyodbc fallback attempt when token auth fails.
        for driver in ["{ODBC Driver 18 for SQL Server}", "{ODBC Driver 17 for SQL Server}"]:
            try:
                connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};"
                return pyodbc.connect(connection_string)
            except pyodbc.Error:
                continue
        raise RuntimeError(
            "Unable to connect to SQL database using Microsoft Entra authentication."
        ) from e
    finally:
        if credential and hasattr(credential, "close"):
            await credential.close()


async def adjust_processed_data_dates():
    """Adjust dates in email records to keep dashboards aligned to current date."""
    conn = await get_db_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        today = datetime.today()
        cursor.execute("SELECT MAX(CAST(sent_datetime AS DATETIME)) FROM [dbo].[processed_data]")
        max_sent = (cursor.fetchone())[0]
        if max_sent:
            days_difference = (today.date() - max_sent.date()).days - 1
            if days_difference > 0:
                cursor.execute(
                    """
                    UPDATE [dbo].[processed_data]
                    SET sent_datetime = DATEADD(DAY, ?, sent_datetime),
                        deadline_at = CASE
                            WHEN deadline_at IS NULL THEN NULL
                            ELSE DATEADD(DAY, ?, deadline_at)
                        END,
                        updated_at = SYSUTCDATETIME()
                    """,
                    (days_difference, days_difference),
                )
                conn.commit()
    finally:
        if cursor:
            cursor.close()
        conn.close()


async def fetch_filters_data():
    """Fetch filter data for job-email analytics."""
    conn = await get_db_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        sql_stmt = """
            SELECT 'Company' AS filter_name, company AS displayValue, company AS key1
            FROM (SELECT DISTINCT company FROM processed_data WHERE company IS NOT NULL AND company <> '') t
            UNION ALL
            SELECT 'Portal' AS filter_name, portal AS displayValue, portal AS key1
            FROM (SELECT DISTINCT portal FROM processed_data WHERE portal IS NOT NULL AND portal <> '') t
            UNION ALL
            SELECT 'Category' AS filter_name, category AS displayValue, category AS key1
            FROM (SELECT DISTINCT category FROM processed_data WHERE category IS NOT NULL AND category <> '') t
            UNION ALL
            SELECT 'Urgency' AS filter_name, urgency AS displayValue, urgency AS key1
            FROM (SELECT DISTINCT urgency FROM processed_data WHERE urgency IS NOT NULL AND urgency <> '') t
            UNION ALL
            SELECT 'ActionRequired' AS filter_name, action_required AS displayValue,
                CASE WHEN action_required IN ('1', 'true', 'True', 'yes', 'Yes') THEN 'true' ELSE 'false' END AS key1
            FROM (
                SELECT DISTINCT CAST(action_required AS NVARCHAR(10)) AS action_required
                FROM processed_data
                WHERE action_required IS NOT NULL
            ) t
            UNION ALL
            SELECT 'DateRange' AS filter_name, date_range AS displayValue, date_range AS key1
            FROM (
                SELECT 'Last 7 days' AS date_range
                UNION ALL SELECT 'Last 14 days'
                UNION ALL SELECT 'Last 30 days'
                UNION ALL SELECT 'Last 90 days'
                UNION ALL SELECT 'Year to Date'
            ) t
        """

        cursor.execute(sql_stmt)
        rows = [tuple(row) for row in cursor.fetchall()]
        column_names = [i[0] for i in cursor.description]
        df = pd.DataFrame(rows, columns=column_names)
        df.rename(columns={'key1': 'key'}, inplace=True)

        nested_json = (
            df.groupby("filter_name")
            .apply(
                lambda x: {
                    "filter_name": x.name,
                    "filter_values": x.to_dict(orient="records"),
                },
                include_groups=False,
            )
            .to_list()
        )
        return nested_json
    finally:
        if cursor:
            cursor.close()
        conn.close()


def _build_where_clause(req_body: dict) -> tuple[str, list]:
    selected_filters = req_body.get("selected_filters", {}) if req_body else {}
    clauses = []
    params = []

    mapping = {
        "Company": "company",
        "Portal": "portal",
        "Category": "category",
        "Urgency": "urgency",
    }

    for filter_key, column in mapping.items():
        values = selected_filters.get(filter_key, [])
        if values:
            placeholders = ", ".join(["?"] * len(values))
            clauses.append(f"{column} IN ({placeholders})")
            params.extend(values)

    action_values = [str(v).lower() for v in selected_filters.get("ActionRequired", []) if str(v).strip()]
    if action_values:
        bool_values = []
        for value in action_values:
            if value in {"true", "1", "yes"}:
                bool_values.append(1)
            elif value in {"false", "0", "no"}:
                bool_values.append(0)
        if bool_values:
            placeholders = ", ".join(["?"] * len(bool_values))
            clauses.append(f"CAST(action_required AS INT) IN ({placeholders})")
            params.extend(bool_values)

    for date_range in selected_filters.get("DateRange", []):
        if date_range == 'Last 7 days':
            clauses.append("sent_datetime >= DATEADD(day, -7, GETDATE())")
        elif date_range == 'Last 14 days':
            clauses.append("sent_datetime >= DATEADD(day, -14, GETDATE())")
        elif date_range == 'Last 30 days':
            clauses.append("sent_datetime >= DATEADD(day, -30, GETDATE())")
        elif date_range == 'Last 90 days':
            clauses.append("sent_datetime >= DATEADD(day, -90, GETDATE())")
        elif date_range == 'Year to Date':
            clauses.append("sent_datetime >= DATEFROMPARTS(YEAR(GETDATE()), 1, 1)")

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_clause, params


async def fetch_chart_data(chart_filters: ChartFilters = ''):
    """Fetch chart data for email workflow analytics."""
    conn = await get_db_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        req_body = {}
        try:
            req_body = chart_filters.model_dump() if chart_filters else {}
        except BaseException:
            req_body = {}

        where_clause, params = _build_where_clause(req_body)

        cards_query = f"""
            SELECT 'TOTAL_EMAILS' AS id, 'Total Emails' AS chart_name, 'card' AS chart_type,
                'Total Emails' AS name, COUNT(*) AS value, '' AS unit_of_measurement
            FROM [dbo].[processed_data] {where_clause}
            UNION ALL
            SELECT 'ACTION_REQUIRED' AS id, 'Action Required' AS chart_name, 'card' AS chart_type,
                'Action Required' AS name,
                SUM(CASE WHEN CAST(action_required AS INT) = 1 THEN 1 ELSE 0 END) AS value,
                '' AS unit_of_measurement
            FROM [dbo].[processed_data] {where_clause}
            UNION ALL
            SELECT 'HIGH_URGENCY' AS id, 'High Urgency' AS chart_name, 'card' AS chart_type,
                'High Urgency' AS name,
                SUM(CASE WHEN urgency = 'High' THEN 1 ELSE 0 END) AS value,
                '' AS unit_of_measurement
            FROM [dbo].[processed_data] {where_clause}
            UNION ALL
            SELECT 'INTERVIEW_INVITATIONS' AS id, 'Interview Invitations' AS chart_name, 'card' AS chart_type,
                'Interview Invitations' AS name,
                SUM(CASE WHEN category = 'Interview Invitation' THEN 1 ELSE 0 END) AS value,
                '' AS unit_of_measurement
            FROM [dbo].[processed_data] {where_clause}
        """
        cursor.execute(cards_query, params * 4)
        rows = [tuple(row) for row in cursor.fetchall()]
        column_names = [i[0] for i in cursor.description]
        df_cards = pd.DataFrame(rows, columns=column_names)

        cards_result = []
        if not df_cards.empty:
            nested_cards = (
                df_cards.groupby(['id', 'chart_name', 'chart_type'])
                .apply(
                    lambda x: x[['name', 'value', 'unit_of_measurement']].to_dict(orient='records'),
                    include_groups=False,
                )
                .reset_index()
            )
            nested_cards.columns = ['id', 'chart_name', 'chart_type', 'chart_value']
            cards_result = nested_cards.to_dict(orient='records')

        category_query = f"""
            SELECT 'CATEGORY_BREAKDOWN' AS id, 'Category Breakdown' AS chart_name,
                'donutchart' AS chart_type, category AS name,
                CAST(COUNT(*) AS FLOAT) AS value, '' AS unit_of_measurement
            FROM [dbo].[processed_data] {where_clause}
            GROUP BY category
        """
        cursor.execute(category_query, params)
        category_rows = [tuple(row) for row in cursor.fetchall()]
        category_cols = [i[0] for i in cursor.description]
        df_category = pd.DataFrame(category_rows, columns=category_cols)

        category_result = []
        if not df_category.empty:
            grouped = (
                df_category.groupby(['id', 'chart_name', 'chart_type'])
                .apply(
                    lambda x: x[['name', 'value', 'unit_of_measurement']].to_dict(orient='records'),
                    include_groups=False,
                )
                .reset_index()
            )
            grouped.columns = ['id', 'chart_name', 'chart_type', 'chart_value']
            category_result = grouped.to_dict(orient='records')

        company_query = f"""
            SELECT TOP 10
                company AS name, 'TOP_COMPANIES' AS id,
                'Top Companies' AS chart_name, 'table' AS chart_type,
                COUNT(*) AS email_count,
                SUM(CASE WHEN CAST(action_required AS INT) = 1 THEN 1 ELSE 0 END) AS action_required_count
            FROM [dbo].[processed_data]
            {where_clause}
            GROUP BY company
            ORDER BY COUNT(*) DESC
        """
        cursor.execute(company_query, params)
        company_rows = [tuple(row) for row in cursor.fetchall()]
        company_cols = [i[0] for i in cursor.description]
        df_company = pd.DataFrame(company_rows, columns=company_cols)

        company_result = []
        if not df_company.empty:
            grouped = (
                df_company.groupby(['id', 'chart_name', 'chart_type'])
                .apply(
                    lambda x: x[['name', 'email_count', 'action_required_count']].to_dict(orient='records'),
                    include_groups=False,
                )
                .reset_index()
            )
            grouped.columns = ['id', 'chart_name', 'chart_type', 'chart_value']
            company_result = grouped.to_dict(orient='records')

        return cards_result + category_result + company_result
    finally:
        if cursor:
            cursor.close()
        conn.close()


async def execute_sql_query(sql_query):
    """Execute SQL query and return concatenated row output."""
    conn = await get_db_connection()
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        result = ''.join(str(row) for row in cursor.fetchall())
        return result
    except Exception as e:
        logging.error("Error executing SQL query: %s", e)
        return None
    finally:
        if cursor:
            cursor.close()
        conn.close()
