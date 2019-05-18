# -*- coding: utf-8 -*-
import pymssql
import mysql.connector
import yaml
from datetime import datetime


class MigrationFromMssqlBase:

    def __init__(self):
        self.mssql_conn = self.__connect_mssql()
        self.mysql_conn = self.__connect_mysql()

        self.mssql_cursor = self.mssql_conn.cursor()
        self.mysql_cursor = self.mysql_conn.cursor()

    def __connect_mssql(self):
        with open('credentials.yml') as file:
            obj = yaml.load(file, Loader=yaml.SafeLoader)
            user = obj['mssql']['user']
            password = obj['mssql']['password']
            hostname = obj['mssql']['hostname']
            dbname = obj['mssql']['dbname']

        return pymssql.connect(server=hostname, user=user,
                               password=password, database=dbname)

    def __connect_mysql(self):
        with open('credentials.yml') as file:
            obj = yaml.load(file, Loader=yaml.SafeLoader)
            user = obj['mysql']['user']
            password = obj['mysql']['password']
            hostname = obj['mysql']['hostname']
            dbname = obj['mysql']['dbname']

        return mysql.connector.connect(user=user, password=password,
                            host=hostname, database=dbname)

    def get_column_encode(self, column_data):
        if column_data is None:
            return ''
        else:
            return column_data.encode('utf-8')

    def get_column_int(self, column_data):
        if column_data is None:
            return 0
        else:
            return column_data

    def get_column_date(self, column_data):
        if column_data is None:
            return datetime(1900, 1, 1)
        else:
            return column_data
