import logging
import sys
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker

# --- CONFIGURATION ---
DB_CONN = "postgresql+psycopg2://airflow:airflow@localhost/airflow"

# EDIT THESE VARIABLES
DAG_ID = "taskgroup_rename_history_demo"
OLD_GROUP_NAME = "fxpm"
NEW_GROUP_NAME = "bonds"
# ---------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("airflow_db_patch")

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

    # Prepare Patterns
    old_prefix = f"{OLD_GROUP_NAME}."
    new_prefix = f"{NEW_GROUP_NAME}."
    like_pattern = f"{old_prefix}%"
    len_old = len(old_prefix) + 1

    params = {
        'dag_id': DAG_ID,
        'new_prefix': new_prefix,
        'len_old': len_old,
        'like_pattern': like_pattern
    }

    try:
        logger.info("Starting Atomic Migration: Cleanup -> Update Parent -> Update Children")

        # --- STEP 1: COLLISION CLEANUP ---
        # If both 'old.task' and 'new.task' exist for the same run, delete the 'old' one.
        # This prevents the Duplicate Key error during the update.
        logger.info("Step 1: Cleaning up collisions in 'task_instance'...")
        cleanup_query = text("""
            DELETE FROM task_instance ti
            USING (
                SELECT dag_id, task_id, run_id, map_index,
                       (:new_prefix || SUBSTRING(task_id FROM :len_old)) AS new_id
                FROM task_instance
                WHERE dag_id = :dag_id AND task_id LIKE :like_pattern
            ) AS sub
            WHERE ti.dag_id = sub.dag_id 
              AND ti.task_id = sub.task_id 
              AND ti.run_id = sub.run_id 
              AND ti.map_index = sub.map_index
              AND EXISTS (
                  SELECT 1 FROM task_instance ti2 
                  WHERE ti2.dag_id = sub.dag_id 
                    AND ti2.task_id = sub.new_id 
                    AND ti2.run_id = sub.run_id 
                    AND ti2.map_index = sub.map_index
              );
        """)
        cleanup_res = session.execute(cleanup_query, params)
        logger.info(f"   -> Removed {cleanup_res.rowcount} colliding old rows.")

        # --- STEP 2: UPDATE PARENT (task_instance) ---
        # We update the parent first so that child FKs have a target to point to.
        logger.info("Step 2: Updating 'task_instance' table...")
        update_ti_query = text("""
            UPDATE task_instance
            SET task_id = :new_prefix || SUBSTRING(task_id FROM :len_old)
            WHERE dag_id = :dag_id AND task_id LIKE :like_pattern
        """)
        ti_res = session.execute(update_ti_query, params)
        logger.info(f"   -> Renamed {ti_res.rowcount} task instances.")

        # --- STEP 3: UPDATE CHILDREN ---
        candidate_tables = [
            'rendered_task_instance_fields', 'xcom', 'task_fail', 
            'task_reschedule', 'task_instance_note', 'task_instance_history', 
            'sla_miss', 'log', 'dataset_event'
        ]
        existing_tables = inspector.get_table_names()
        
        for table in [t for t in candidate_tables if t in existing_tables]:
            t_col = 'source_task_id' if table == 'dataset_event' else 'task_id'
            d_col = 'source_dag_id' if table == 'dataset_event' else 'dag_id'

            logger.info(f"Step 3: Updating child table '{table}'...")
            
            # Sub-step: Delete child collisions if they exist
            # (e.g., if XComs already exist for the new task name)
            child_cleanup = text(f"""
                DELETE FROM {table} t1
                WHERE {d_col} = :dag_id AND {t_col} LIKE :like_pattern
                AND EXISTS (
                    SELECT 1 FROM {table} t2
                    WHERE t2.{d_col} = t1.{d_col}
                      AND t2.{t_col} = :new_prefix || SUBSTRING(t1.{t_col} FROM :len_old)
                      {"AND t2.run_id = t1.run_id" if 'run_id' in [c['name'] for c in inspector.get_columns(table)] else ""}
                      {"AND t2.key = t1.key" if 'key' in [c['name'] for c in inspector.get_columns(table)] else ""}
                      {"AND t2.map_index = t1.map_index" if 'map_index' in [c['name'] for c in inspector.get_columns(table)] else ""}
                )
            """)
            session.execute(child_cleanup, params)

            # Perform the actual rename
            child_update = text(f"""
                UPDATE {table}
                SET {t_col} = :new_prefix || SUBSTRING({t_col} FROM :len_old)
                WHERE {d_col} = :dag_id AND {t_col} LIKE :like_pattern
            """)
            res = session.execute(child_update, params)
            logger.info(f"   -> Updated {res.rowcount} rows in '{table}'.")

        # --- COMMIT ALL CHANGES ---
        session.commit()
        logger.info("SUCCESS: All history migrated.")

    except Exception as e:
        session.rollback()
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error("Transaction rolled back.")
    finally:
        session.close()

if __name__ == "__main__":
    print(f"--- Airflow TaskGroup Renamer (Cleanup & Update) ---")
    confirm = input(f"Rename {OLD_GROUP_NAME} to {NEW_GROUP_NAME} in DAG {DAG_ID}? (yes/no): ")
    if confirm.lower() == "yes":
        run_migration()
