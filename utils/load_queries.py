import logging

logging.basicConfig(level = logging.INFO)
def load_queries():
    try:
        with open('sql/create_table.sql', 'r') as file:
            create_table_query = file.read()

        with open('sql/insert_data.sql', 'r') as file:
            insert_data_query = file.read()
        
        return create_table_query, insert_data_query
    
    except FileNotFoundError as e:
        logging.error(f'SQL file not found: {e}')
        exit()   

    except Exception as e:
        logging.error(f'Error reading SQL file: {e}')
        exit() 