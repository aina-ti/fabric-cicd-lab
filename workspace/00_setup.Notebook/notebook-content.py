# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "30bd74f9-927b-4c69-a1dc-2d46143ee6ca",
# META       "default_lakehouse_name": "treino",
# META       "default_lakehouse_workspace_id": "df50ac38-5cee-4716-927d-727a7f308ea7"
# META     }
# META   }
# META }

# CELL ********************

# Practice tables with the same traps as treino_sql. Rerun at any time to reset them.
from datetime import date, datetime as dt

customers_raw = [
    (1, 101, 'Ana Souza', 'ana@mail.com', 'Sao Paulo', dt(2026, 1, 10, 9, 0)),
    (2, 101, 'Ana Souza', 'ana@mail.com', 'Sao Paulo', dt(2026, 1, 10, 9, 0)),
    (3, 101, 'Ana Souza', 'ana.souza@mail.com', 'Campinas', dt(2026, 3, 2, 14, 30)),
    (4, 102, 'Bruno Lima', 'bruno@mail.com', 'Rio de Janeiro', dt(2026, 2, 1, 8, 0)),
    (5, 102, 'Bruno Lima', None, 'Niteroi', dt(2026, 2, 15, 10, 0)),
    (6, 103, 'Carla Dias', 'carla@mail.com', 'Recife', dt(2026, 1, 20, 12, 0)),
    (7, 103, 'Carla Dias', 'carla@mail.com', 'Olinda', dt(2026, 1, 20, 12, 0)),
    (8, 104, 'Diego Alves', 'diego@mail.com', 'Curitiba', None),
    (9, 104, 'Diego Alves', 'diego.alves@mail.com', 'Curitiba', dt(2026, 2, 10, 16, 45)),
    (10, 105, 'Elisa Reis', 'elisa@mail.com', 'Salvador', dt(2026, 3, 5, 11, 15)),
]

customer_current = [
    (101, 'Ana Souza', 'Sao Paulo', 'Retail', date(2026, 2, 28)),
    (102, 'Bruno Lima', 'Sao Paulo', 'Retail', date(2026, 2, 28)),
    (103, 'Carla Dias', 'Recife', 'Retail', date(2026, 2, 28)),
    (104, 'Diego Alves', 'Curitiba', 'Corporate', date(2026, 2, 28)),
    (105, 'Elisa Reis', 'Salvador', None, date(2026, 2, 28)),
    (106, 'Fabio Nunes', None, None, date(2026, 2, 28)),
]

OPEN = date(9999, 12, 31)
dim_customer = [
    (1, 101, 'Ana Souza', 'Sao Paulo', 'Retail', date(2025, 1, 1), OPEN, True),
    (2, 102, 'Bruno Lima', 'Rio de Janeiro', 'Retail', date(2025, 1, 1), date(2025, 6, 30), False),
    (3, 102, 'Bruno Lima', 'Sao Paulo', 'Retail', date(2025, 7, 1), OPEN, True),
    (4, 103, 'Carla Dias', 'Recife', 'Retail', date(2025, 1, 1), OPEN, True),
    (5, 104, 'Diego Alves', 'Curitiba', 'Corporate', date(2025, 1, 1), OPEN, True),
    (6, 105, 'Elisa Reis', 'Salvador', None, date(2025, 1, 1), OPEN, True),
    (7, 106, 'Fabio Nunes', None, None, date(2025, 1, 1), OPEN, True),
]

load = date(2026, 3, 31)
stg_customers = [
    (101, 'Ana Souza', 'Sao Paulo', 'Retail', load),
    (102, 'Bruno Lima', 'Niteroi', 'Retail', load),
    (103, 'Carla Dias', 'Recife', 'Private', load),
    (105, 'Elisa Reis', 'Salvador', 'Retail', load),
    (106, 'Fabio Nunes', None, None, load),
    (107, 'Gabi Rocha', 'Fortaleza', 'Retail', load),
]

# Incremental load: events arrive out of order; event_id 7 is a late arrival.
events = [
    (1, 101, dt(2026, 3, 1, 8, 0), 100.0),
    (2, 102, dt(2026, 3, 1, 9, 0), 250.0),
    (3, 101, dt(2026, 3, 1, 10, 0), 75.0),
    (3, 101, dt(2026, 3, 1, 10, 0), 75.0),
    (4, 103, dt(2026, 3, 2, 8, 30), 300.0),
    (5, 102, dt(2026, 3, 2, 11, 0), 50.0),
    (6, 104, dt(2026, 3, 3, 9, 15), 500.0),
    (7, 105, dt(2026, 3, 1, 23, 0), 40.0),
]

tables = {
    'customers_raw': (customers_raw, 'load_id int, customer_id int, full_name string, email string, city string, updated_at timestamp'),
    'customer_current': (customer_current, 'customer_id int, full_name string, city string, segment string, last_load date'),
    'dim_customer': (dim_customer, 'customer_sk long, customer_id int, full_name string, city string, segment string, valid_from date, valid_to date, is_current boolean'),
    'stg_customers': (stg_customers, 'customer_id int, full_name string, city string, segment string, load_date date'),
    'events': (events, 'event_id int, customer_id int, event_ts timestamp, amount double'),
}

for name, (rows, schema) in tables.items():
    spark.createDataFrame(rows, schema).write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(name)
    print(name, spark.table(name).count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
