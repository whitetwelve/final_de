import configparser
import snowflake.connector


class SnowflakeTL:

    def __init__(self, credentials):
        self.user = credentials['snowflake']['user']
        self.password = credentials['snowflake']['password']
        self.account = credentials['snowflake']['account']
        self.warehouse = credentials['snowflake']['warehouse']
        self.database = credentials['snowflake']['database']

    def Send(self, query):
        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
        )

        curs = conn.cursor()

        try:
            curs.execute(query)
        finally:
            curs.close()

        conn.close()
