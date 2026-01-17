child_tables = [
        # (table_name, dag_id_col, task_id_col, run_id_col)
        ("xcom", "dag_id", "task_id", "dag_run_id"),
        ("rendered_task_instance_fields", "dag_id", "task_id", "run_id"),
        ("task_reschedule", "dag_id", "task_id", "run_id"),
        ("task_fail", "dag_id", "task_id", "run_id"),
        ("task_instance_note", "dag_id", "task_id", "run_id"),
        ("task_instance_history", "dag_id", "task_id", "run_id"),
        ("sla_miss", "dag_id", "task_id", None), # No run_id link
        ("log", "dag_id", "task_id", None),      # No run_id link
    ]



logger.info("Step 2: Updating child tables (skipping duplicates)...")
        for table_info in active_children:
            # Unpack the new 4-item tuple
            table, dag_col, task_col, run_id_col = table_info
            
            # Base query
            sql_str = f"""
                UPDATE {table} AS target
                SET {task_col} = :new_prefix || SUBSTRING({task_col} FROM :len_old)
                WHERE {dag_col} = :dag_id AND {task_col} LIKE :like_pattern
            """

            # If the table has a Run ID column, add the duplicate check
            if run_id_col:
                sql_str += f"""
                AND NOT EXISTS (
                    SELECT 1 FROM {table} AS sub
                    WHERE sub.{dag_col} = target.{dag_col}
                      AND sub.{run_id_col} = target.{run_id_col}
                      AND sub.{task_col} = :new_prefix || SUBSTRING(target.{task_col} FROM :len_old)
                      {"AND sub.map_index = target.map_index" if table == 'xcom' else ""}
                      {"AND sub.key = target.key" if table == 'xcom' else ""}
                )
                """

            update_sql = text(sql_str)
            upd_res = session.execute(update_sql, params)
            logger.info(f"   -> Updated {upd_res.rowcount} rows in {table}.")
