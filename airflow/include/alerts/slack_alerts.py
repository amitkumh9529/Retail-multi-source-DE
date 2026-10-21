"""
RetailPulse Production Airflow Alerting Callbacks.

Formats and sends rich Slack webhook notifications on task failures, retries, and SLA misses.
"""
import logging
import os
import requests

logger = logging.getLogger("retailpulse.alerts")


def send_slack_alert(context: dict, event_type: str = "FAILURE"):
    """
    Airflow callback for task failures or SLA misses.
    Extracts execution context, dag_id, task_id, exception, and log URL.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url or "hooks.slack.com" not in webhook_url:
        logger.warning(f"[Alert] No SLACK_WEBHOOK_URL configured. Logging {event_type} locally.")
        logger.error(
            f"[{event_type}] DAG: {context.get('dag').dag_id} | "
            f"Task: {context.get('task_instance').task_id} | "
            f"Exec Date: {context.get('execution_date')} | "
            f"Exception: {context.get('exception')}"
        )
        return

    ti = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception", "No exception specified")
    log_url = ti.log_url if ti else "N/A"

    color = "#E01E5A" if event_type == "FAILURE" else "#ECB22E"
    title = f":rotating_light: Pipeline Alert: {event_type} in {dag.dag_id}"

    payload = {
        "text": title,
        "attachments": [
            {
                "color": color,
                "fields": [
                    {"title": "DAG", "value": dag.dag_id, "short": True},
                    {"title": "Task", "value": ti.task_id if ti else "N/A", "short": True},
                    {"title": "Execution Date", "value": str(context.get("execution_date")), "short": True},
                    {"title": "Try Number", "value": str(ti.try_number if ti else 1), "short": True},
                    {"title": "Error Message", "value": f"```{str(exception)[:400]}```", "short": False},
                    {"title": "Task Log URL", "value": f"<{log_url}|View Airflow Task Log>", "short": False},
                ],
                "footer": "RetailPulse Production Data Engineering Observability",
            }
        ],
    }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info(f"[Alert] Successfully dispatched {event_type} notification to Slack.")
    except Exception as e:
        logger.error(f"[Alert] Failed dispatching Slack alert: {e}")


def on_failure_callback(context: dict):
    send_slack_alert(context, event_type="FAILURE")


def on_retry_callback(context: dict):
    send_slack_alert(context, event_type="RETRY")


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.error(f"[SLAMiss] SLA missed for DAG: {dag.dag_id} on tasks: {task_list}")
