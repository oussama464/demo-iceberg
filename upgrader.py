import logging
import sys
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
### uv add sqlalchemy psycopg2-binary
# --- CONFIGURATION ---
# Your Connection String
DB_CONN = "postgresql+psycopg2://airflow:airflow@localhost/airflow"

# EDIT THESE VARIABLES
DAG_ID = "taskgroup_rename_history_demo"         
OLD_GROUP_NAME = "fxpm"     
NEW_GROUP_NAME = "bonds"     
# ---------------------

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("airflow_db_patch")

def get_target_tables(inspector):
    """
    Defines the tables to update.
    Returns a list of tuples: (table_name, task_id_column, dag_id_column)
    """
    candidate_tables = {
        'task_instance': ('task_id', 'dag_id'),
        'task_instance_history': ('task_id', 'dag_id'), 
        'task_fail': ('task_id', 'dag_id'),
        'task_reschedule': ('task_id', 'dag_id'),
        'xcom': ('task_id', 'dag_id'),
        'sla_miss': ('task_id', 'dag_id'),
        'rendered_task_instance_fields': ('task_id', 'dag_id'), # The culprit table
        'task_instance_note': ('task_id', 'dag_id'),
        'log': ('task_id', 'dag_id'),
        'dataset_event': ('source_task_id', 'source_dag_id') 
    }
    
    existing_tables = inspector.get_table_names()
    valid_tables = []
    
    for table, (task_col, dag_col) in candidate_tables.items():
        if table in existing_tables:
            valid_tables.append((table, task_col, dag_col))
            
    return valid_tables

def run_migration():
    if OLD_GROUP_NAME == NEW_GROUP_NAME:
        logger.error("Old and New Group Names are the same. Nothing to do.")
        return

    try:
        engine = create_engine(DB_CONN)
        Session = sessionmaker(bind=engine)
        session = Session()
        inspector = inspect(engine)
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)

    old_prefix = f"{OLD_GROUP_NAME}."
    new_prefix = f"{NEW_GROUP_NAME}."
    like_pattern = f"{old_prefix}%"
    len_old = len(old_prefix) + 1

    logger.info(f"Connected to DB. Preparing to rename '{old_prefix}*' -> '{new_prefix}*' for DAG '{DAG_ID}'")

    tables = get_target_tables(inspector)
    
    try:
        # --- FIX: BYPASS CONSTRAINTS START ---
        # We set the session role to 'replica' which disables triggers (and FK checks)
        # This requires the DB user to have sufficient privileges (usually superuser).
        logger.info("Temporarily disabling Foreign Key checks (session_replication_role)...")
        session.execute(text("SET session_replication_role = 'replica';"))
        # -------------------------------------

        total_rows = 0
        for table_name, task_col, dag_col in tables:
            query = text(f"""
                UPDATE {table_name}
                SET {task_col} = :new_prefix || SUBSTRING({task_col} FROM :len_old)
                WHERE {dag_col} = :dag_id 
                  AND {task_col} LIKE :like_pattern
            """)

            result = session.execute(query, {
                'dag_id': DAG_ID,
                'new_prefix': new_prefix,
                'len_old': len_old,
                'like_pattern': like_pattern
            })
            
            if result.rowcount > 0:
                logger.info(f"Updated {result.rowcount} rows in '{table_name}'")
                total_rows += result.rowcount

        # --- FIX: RESTORE CONSTRAINTS ---
        logger.info("Restoring Foreign Key checks...")
        session.execute(text("SET session_replication_role = 'origin';"))
        # --------------------------------

        if total_rows == 0:
            logger.warning("No rows matched the criteria. Check your DAG_ID and Group Names.")
            session.rollback()
        else:
            session.commit()
            logger.info(f"SUCCESS: Transaction Committed. Total rows updated: {total_rows}")

    except Exception as e:
        session.rollback()
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error("Transaction rolled back. No changes were made.")
    finally:
        session.close()

if __name__ == "__main__":
    print(f"--- Airflow TaskGroup Renamer (Fixed) ---")
    print(f"Target DB: {DB_CONN}")
    print(f"DAG: {DAG_ID} | Rename: {OLD_GROUP_NAME} -> {NEW_GROUP_NAME}")
    
    confirm = input("\nType 'yes' to proceed with the database update: ")
    if confirm.lower() == "yes":
        run_migration()
    else:
        print("Cancelled.")