import logging
import sys
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

# --- CONFIGURATION ---
DB_CONN = "postgresql+psycopg2://airflow:airflow@localhost/airflow"
DAG_ID = "taskgroup_rename_history_demo"
OLD_GROUP_NAME = "fxpm"
NEW_GROUP_NAME = "bonds"
# ---------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("airflow_db_patch")

def get_ti_columns(inspector):
    """Get all columns for task_instance to allow row cloning."""
    return [c['name'] for c in inspector.get_columns('task_instance')]

def run_migration():
    if OLD_GROUP_NAME == NEW_GROUP_NAME:
        logger.error("Old and New Group Names are the same.")
        return

    try:
        engine = create_engine(DB_CONN)
        Session = sessionmaker(bind=engine)
        session = Session()
        inspector = inspect(engine)
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)

    params = {
        "dag_id": DAG_ID,
        "new_prefix": f"{NEW_GROUP_NAME}.",
        "len_old": len(f"{OLD_GROUP_NAME}.") + 1,
        "like_pattern": f"{OLD_GROUP_NAME}.%",
    }

    # Tables referencing task_instance
    child_tables = [
        ("xcom", "dag_id", "task_id"),
        ("rendered_task_instance_fields", "dag_id", "task_id"),
        ("task_reschedule", "dag_id", "task_id"),
        ("task_fail", "dag_id", "task_id"),
        ("task_instance_note", "dag_id", "task_id"),
        ("task_instance_history", "dag_id", "task_id"),
        ("sla_miss", "dag_id", "task_id"),
        ("log", "dag_id", "task_id"),
        ("dataset_event", "source_dag_id", "source_task_id"),
    ]

    existing_tables = set(inspector.get_table_names())
    active_children = [t for t in child_tables if t[0] in existing_tables]

    try:
        logger.info("Step 1: Cloning task_instance rows to satisfy FKs...")
        ti_cols = get_ti_columns(inspector)
        
        # Build column list for INSERT, replacing task_id with the new name
        col_names = ", ".join(ti_cols)
        select_clause = ", ".join([
            f":new_prefix || SUBSTRING(task_id FROM :len_old)" if c == 'task_id' else c 
            for c in ti_cols
        ])

        # We use ON CONFLICT DO NOTHING in case Step 1 was partially run before
        clone_sql = text(f"""
            INSERT INTO task_instance ({col_names})
            SELECT {select_clause} FROM task_instance
            WHERE dag_id = :dag_id AND task_id LIKE :like_pattern
            ON CONFLICT DO NOTHING
        """)
        
        clone_res = session.execute(clone_sql, params)
        logger.info(f"   -> Cloned {clone_res.rowcount} rows in task_instance.")

        logger.info("Step 2: Updating child tables (preserving history)...")
        for table, dag_col, task_col in active_children:
            update_sql = text(f"""
                UPDATE {table}
                SET {task_col} = :new_prefix || SUBSTRING({task_col} FROM :len_old)
                WHERE {dag_col} = :dag_id AND {task_col} LIKE :like_pattern
            """)
            upd_res = session.execute(update_sql, params)
            logger.info(f"   -> Updated {upd_res.rowcount} rows in {table}.")

        logger.info("Step 3: Cleaning up old task_instance rows...")
        cleanup_sql = text("""
            DELETE FROM task_instance
            WHERE dag_id = :dag_id AND task_id LIKE :like_pattern
        """)
        del_res = session.execute(cleanup_sql, params)
        logger.info(f"   -> Deleted {del_res.rowcount} old task_instance rows.")

        session.commit()
        logger.info("SUCCESS: Migration complete.")

    except Exception as e:
        session.rollback()
        logger.error(f"CRITICAL ERROR: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    run_migration()
