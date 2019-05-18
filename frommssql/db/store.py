from . import mysql_base
from .. import data


class StoreDao(mysql_base.MysqlBase):

    def get_all(self):

        sql = self.__get_sql_select()
        sql = sql + " ORDER BY created_at "

        self.cursor.execute(sql)

        rs = self.cursor.fetchall()

        javs = self.__get_list(rs)

        if javs is None or len(javs) <= 0:
            return None

        return javs

    def get_where_agreement(self, where):

        sql = self.__get_sql_select()
        sql = sql + where

        self.mysql_cursor.execute(sql)

        rs = self.cursor.fetchall()

        javs = self.__get_list(rs)

        if javs is None or len(javs) <= 0:
            return None

        return javs

    def __get_sql_select(self):

        sql = 'SELECT id' \
              '  , label, name1, name2, path ' \
              '  , remark' \
              '  , created_at, updated_at ' \
              '  FROM store '

        return sql

    def __get_list(self, rs):

        stores = []
        for row in rs:
            store = data.StoreData()
            store.id = row[0]
            store.label = row[1]
            store.name1 = row[2]
            store.name2 = row[3]
            store.path = row[4]
            store.remark = row[5]
            store.createdAt = row[6]
            store.updatedAt = row[7]
            stores.append(store)

        return stores

