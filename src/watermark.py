"""增量水位管理：记录每个 ETL 任务处理到哪个放款月，支持增量与回补。"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from src.utils import get_engine, get_logger

log = get_logger("watermark")


def get_watermark(job_name: str) -> str | None:
    with get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT watermark_value FROM etl_watermark WHERE job_name=:j"),
            {"j": job_name}).fetchone()
    return row[0] if row else None


def set_watermark(job_name: str, value: str, rows: int) -> None:
    with get_engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_watermark
              (job_name, watermark_value, last_run_time, last_run_rows)
            VALUES (:j, :v, :t, :r)
            ON DUPLICATE KEY UPDATE
              watermark_value = VALUES(watermark_value),
              last_run_time   = VALUES(last_run_time),
              last_run_rows   = VALUES(last_run_rows)
        """), {"j": job_name, "v": str(value), "t": datetime.now(), "r": rows})
    log.info(f"水位更新 {job_name} -> {value}（本次 {rows:,} 行）")


def report() -> None:
    with get_engine().connect() as conn:
        rows = conn.execute(text(
            "SELECT job_name, watermark_value, last_run_time, last_run_rows "
            "FROM etl_watermark ORDER BY job_name")).fetchall()
    if not rows:
        log.info("水位表为空")
        return
    log.info("当前水位：")
    for r in rows:
        log.info(f"  {r[0]:20} {r[1]:10} {r[2]} {r[3]:,} 行")


if __name__ == "__main__":
    report()
