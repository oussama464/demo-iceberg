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

def get_ti_columns(inspector):
    """
    Gets the column names for task_instance, excluding the auto-increment 'id' 
    so we can clone rows without Primary Key collisions.
    """
    columns = [c['name'] for c in inspector.get_columns('task_instance')]
    # Exclude 'id' if it exists (it's a surrogate key in newer Airflow versions)
    # Postgres will auto-generate a new ID for the cloned row.
    return [c for c in columns if c != 'id']

def get_child_tables(inspector):
    """
    Returns the list of child tables that reference task_instance
    and need their task_id updated.
    """
    # These tables typically link to task_instance via (dag_id, task_id, run_id, map_index)
    candidate_tables = [
        'rendered_task_instance_fields', # The strict FK table
        'task_instance_history',         # Airflow 2.10+
        'task_fail',
        'task_reschedule',
        'xcom',
        'task_instance_note',
        'sla_miss',
        'log',                           # Often no FK, but good to update
        'dataset_event'                  # Check column: source_task_id
    ]
    
    existing_tables = inspector.get_table_names()
    return [t for t in candidate_tables if t in existing_tables]

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

    # Prepare Patterns
    old_prefix = f"{OLD_GROUP_NAME}."
    new_prefix = f"{NEW_GROUP_NAME}."
    like_pattern = f"{old_prefix}%"
    len_old = len(old_prefix) + 1

    logger.info(f"Connected. Strategy: Clone TI -> Switch Children -> Delete Old TI")
    
    try:
        # --- STEP 1: CLONE PARENT (task_instance) ---
        # We copy the old rows, creating new ones with the new task_id.
        # This satisfies the "Parent must exist" requirement for FKs.
        
        ti_cols = get_ti_columns(inspector)
        col_list = ", ".join(ti_cols)
        
        # Build dynamic SELECT list, replacing task_id logic
        select_parts = []
        for col in ti_cols:
            if col == 'task_id':
                select_parts.append(f":new_prefix || SUBSTRING(task_id FROM :len_old)")
            else:
                select_parts.append(col)
        select_list = ", ".join(select_parts)

        logger.info("Step 1: Cloning 'task_instance' rows with new names...")
        clone_query = text(f"""
            INSERT INTO task_instance ({col_list})
            SELECT {select_list}
            FROM task_instance
            WHERE dag_id = :dag_id AND task_id LIKE :like_pattern
        """)
        
        clone_result = session.execute(clone_query, {
            'dag_id': DAG_ID, 'new_prefix': new_prefix,
            'len_old': len_old, 'like_pattern': like_pattern
        })
        logger.info(f"Cloned {clone_result.rowcount} rows in task_instance.")

        if clone_result.rowcount == 0:
            logger.warning("No tasks found to rename. rolling back.")
            session.rollback()
            return

        # --- STEP 2: SWITCH CHILDREN ---
        # Update all child tables to point to the NEW task_id.
        # Since we just created the new TIs in Step 1, the Foreign Keys are happy.
        
        child_tables = get_child_tables(inspector)
        logger.info(f"Step 2: Switching pointers in {len(child_tables)} child tables...")
        
        for table in child_tables:
            # Handle dataset_event special column naming
            t_col = 'source_task_id' if table == 'dataset_event' else 'task_id'
            d_col = 'source_dag_id' if table == 'dataset_event' else 'dag_id'

            update_query = text(f"""
                UPDATE {table}
                SET {t_col} = :new_prefix || SUBSTRING({t_col} FROM :len_old)
                WHERE {d_col} = :dag_id AND {t_col} LIKE :like_pattern
            """)
            
            res = session.execute(update_query, {
                'dag_id': DAG_ID, 'new_prefix': new_prefix,
                'len_old': len_old, 'like_pattern': like_pattern
            })
            if res.rowcount > 0:
                logger.info(f"   -> Updated {res.rowcount} rows in '{table}'")

        # --- STEP 3: DELETE OLD PARENT ---
        # Now that children point to the new rows, the old rows have no dependencies.
        # We can safely delete them.
        
        logger.info("Step 3: Deleting old 'task_instance' rows...")
        delete_query = text("""
            DELETE FROM task_instance
            WHERE dag_id = :dag_id AND task_id LIKE :like_pattern
        """)
        del_result = session.execute(delete_query, {
            'dag_id': DAG_ID, 'like_pattern': like_pattern
        })
        logger.info(f"Deleted {del_result.rowcount} old rows from task_instance.")

        # --- COMMIT ---
        session.commit()
        logger.info("SUCCESS: Migration complete. History preserved.")

    except Exception as e:
        session.rollback()
        logger.error(f"CRITICAL ERROR: {e}")
        logger.error("Transaction rolled back. No changes were made.")
    finally:
        session.close()

if __name__ == "__main__":
    print(f"--- Airflow TaskGroup Renamer (Clone-Switch-Delete) ---")
    print(f"Target DB: {DB_CONN}")
    print(f"DAG: {DAG_ID} | Rename: {OLD_GROUP_NAME} -> {NEW_GROUP_NAME}")
    
    confirm = input("\nType 'yes' to proceed: ")
    if confirm.lower() == "yes":
        run_migration()
    else:
        print("Cancelled.")
