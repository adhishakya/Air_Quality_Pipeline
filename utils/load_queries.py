import os
import logging

logging.basicConfig(level = logging.INFO)
def load_queries():
    try:
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sql_dir = os.path.join(root_dir, 'sql')

        with open(os.path.join(sql_dir,'create_table.sql'), 'r') as file:
            create_table_query = file.read()
            print(create_table_query)

        with open(os.path.join(sql_dir,'insert_data.sql'), 'r') as file:
            insert_data_query = file.read()
            print(insert_data_query)
        
        return create_table_query, insert_data_query
    
    except FileNotFoundError as e:
        logging.error(f'SQL file not found: {e}')
        exit()   

    except Exception as e:
        logging.error(f'Error reading SQL file: {e}')
        exit() 