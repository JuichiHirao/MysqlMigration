from . import mysql_base
from .. import data


class FavDao(mysql_base.MysqlBase):

    def get_all(self):

        sql = self.__get_sql_select()
        sql = sql + " ORDER BY created_at "

        self.cursor.execute(sql)

        rs = self.cursor.fetchall()

        javs = self.__get_list(rs)

        if javs is None or len(javs) <= 0:
            return None

        return javs

    def __get_sql_select(self):

        sql = 'SELECT id' \
              '  , label, name, type, comment ' \
              '  , remark' \
              '  , created_at, updated_at ' \
              '  FROM fav '

        return sql

    def __get_list(self, rs):

        favs = []
        for row in rs:
            fav = data.FavData()
            fav.id = row[0]
            fav.label = row[1]
            fav.name = row[2]
            fav.type = row[3]
            fav.comment = row[4]
            fav.remark = row[5]
            fav.createdAt = row[6]
            fav.updatedAt = row[7]
            favs.append(fav)

        return favs

    def get_where(self, where, params):

        sql = self.__get_sql_select()
        sql = sql + where

        if params is None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, params)

        # rowcountは戻りがあっても、正しい件数を取得出来ない
        # rowcount = self.cursor.rowcount
        rs = self.cursor.fetchall()

        jav2s = self.__get_list(rs)

        self.conn.commit()

        return jav2s

    def export(self, data: data.FavData = None):

        sql = 'INSERT INTO fav(label ' \
              ', name, type, comment, remark ' \
              ', created_at, updated_at) ' \
              ' VALUES(%s ' \
              ', %s, %s, %s, %s ' \
              ', %s, %s)'

        self.cursor.execute(sql, (data.label
                                        , data.name, data.type, data.comment, data.remark
                                        , data.createdAt, data.updatedAt))

        self.conn.commit()
