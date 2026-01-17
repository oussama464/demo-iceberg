# --- STEP 2: SWITCH CHILDREN ---
        child_tables = get_child_tables(inspector)
        logger.info(f"Step 2: Switching pointers in {len(child_tables)} child tables...")
        
        for table in child_tables:
            # Handle dataset_event special column naming
            t_col = 'source_task_id' if table == 'dataset_event' else 'task_id'
            d_col = 'source_dag_id' if table == 'dataset_event' else 'dag_id'

            # --- DYNAMIC COLLISION CHECK ---
            # We must inspect the table to see which columns define "uniqueness" (PK)
            # so we can build a "NOT EXISTS" clause.
            tbl_cols = [c['name'] for c in inspector.get_columns(table)]
            
            # Build conditions to match the row (DAG + Run + other keys)
            match_criteria = [f"t2.{d_col} = {table}.{d_col}"] 

            # Add relevant columns if they exist in this table
            if 'run_id' in tbl_cols:
                match_criteria.append(f"t2.run_id = {table}.run_id")
            if 'map_index' in tbl_cols:
                match_criteria.append(f"t2.map_index = {table}.map_index")
            if 'key' in tbl_cols:      # Critical for XComs
                match_criteria.append(f"t2.key = {table}.key")
            if 'try_number' in tbl_cols: # Helpful for task_fail / task_reschedule
                match_criteria.append(f"t2.try_number = {table}.try_number")

            match_sql = " AND ".join(match_criteria)

            # This clause prevents the UPDATE if the target row already exists
            not_exists_clause = f"""
                AND NOT EXISTS (
                    SELECT 1 FROM {table} t2
                    WHERE t2.{t_col} = :new_prefix || SUBSTRING({table}.{t_col} FROM :len_old)
                    AND {match_sql}
                )
            """

            update_query = text(f"""
                UPDATE {table}
                SET {t_col} = :new_prefix || SUBSTRING({t_col} FROM :len_old)
                WHERE {d_col} = :dag_id AND {t_col} LIKE :like_pattern
                {not_exists_clause}
            """)
            
            res = session.execute(update_query, {
                'dag_id': DAG_ID, 'new_prefix': new_prefix,
                'len_old': len_old, 'like_pattern': like_pattern
            })
            if res.rowcount > 0:
                logger.info(f"   -> Updated {res.rowcount} rows in '{table}'")
            else:
                logger.info(f"   -> No eligible rows updated in '{table}' (orphans or collisions skipped)")
