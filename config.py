from environs import Env

env = Env()
env.read_env()

DB_HOST = env.str("DB_HOST")
DB_PORT = env.int("DB_PORT")
DB_USERNAME = env.str("DB_USERNAME")
DB_PASSWORD = env.str("DB_PASSWORD")
DB_NAME = env.str("DB_NAME")
