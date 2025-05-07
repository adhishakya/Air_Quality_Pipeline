import logging
from utils.db_connection import get_db_connection
from utils.load_queries import load_queries

def load_data(ti):
    json_data = ti.xcom_pull(task_ids='transform_data')

    try:
        with get_db_connection() as conn: 
            with conn.cursor() as cursor: 
                create_table_query, insert_data_query = load_queries()

                cursor.execute(create_table_query)
                logging.info('Table exists or created successfully')

                values = [
                    (
                        value['City'],
                        value['Country'],
                        value['AQI'],
                        value['AQI_Timestamp'],
                        value['Remarks']
                    )
                    for value in json_data
                ]

                try:    
                    cursor.executemany(insert_data_query, values) 
                    conn.commit()
                    logging.info('Data inserted successfully')

                except Exception as e:
                    logging.error(f'Error inserting data: {e}')
                    exit()

    except Exception as e:
        logging.error(f'Error while inserting data into database: {e}')
