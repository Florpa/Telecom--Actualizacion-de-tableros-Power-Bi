# -*- coding: UTF-8 -*-

# este script podría llamarse también settings.py

import os

# from dotenv import load_dotenv
# load_dotenv(verbose=True)

# postgres keys
godatos_pg_user = os.environ.get('GODATOS_PG_USER')
godatos_pg_password = os.environ.get('GODATOS_PG_PASSWORD')
godatos_pg_host = os.environ.get('GODATOS_PG_HOST')
godatos_pg_port = os.environ.get('GODATOS_PG_PORT')
godatos_pg_db_name = os.environ.get('GODATOS_DB_NAME')

# os.getenv('API_GOSIT_USER')
# al parecer funciona igual
# para que al iniciar desde consola me tomen las environment variables debo hacer export key=value
# "child processes"
