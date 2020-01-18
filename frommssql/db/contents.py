from . import mysql_base
from .. import data


class ContentsDao(mysql_base.MysqlBase):

    def get_all(self):

        sql = self.__get_sql_select()
        sql = sql + " ORDER BY created_at "

        self.cursor.execute(sql)

        rs = self.cursor.fetchall()

        contents_list = self.__get_list(rs)

        if contents_list is None or len(contents_list) <= 0:
            return None

        return contents_list

    def get_where_agreement(self, where: str = '', param_list: list = None):

        sql = self.__get_sql_select(where)

        if param_list:
            self.cursor.execute(sql, param_list)
        else:
            self.cursor.execute(sql)

        rs = self.cursor.fetchall()

        contents_list = self.__get_list(rs)

        if contents_list is None or len(contents_list) <= 0:
            return None

        return contents_list

    def __get_sql_select(self, where: str = ''):

        sql = 'SELECT id' \
              '  , store_label, name, product_number, extension ' \
              '  , tag, publish_date, file_date, file_count ' \
              '  , size, rating, comment, remark' \
              '  , file_status ' \
              '  , created_at, updated_at ' \
              '  FROM contents '

        if len(where) > 0:
            sql = sql + where + ' ORDER BY id '
        else:
            sql = sql + 'WHERE deleted = 0 ORDER BY id '

        return sql

    def __get_list(self, rs):

        contents_list = []
        for row in rs:
            contents = data.ContentsData()
            contents.id = row[0]
            contents.store_label = row[1]
            contents.name = row[2]
            contents.productNumber = row[3]
            contents.extension = row[4]
            contents.tag = row[5]
            contents.publishDate = row[6]
            contents.fileDate = row[6]
            contents.fileCount = row[7]
            contents.size = row[8]
            contents.rating = row[9]
            contents.rating = row[10]
            contents.comment = row[11]
            contents.remark = row[12]
            contents.isNotExist = row[13]
            contents.createdAt = row[14]
            contents.updatedAt = row[15]
            contents_list.append(contents)

        return contents_list

    def update_tag(self, register_data: data.ContentsData = None):

        sql = 'UPDATE contents ' \
              '  SET tag = %s ' \
              ' WHERE id = %s '

        self.cursor.execute(sql, (register_data.tag, register_data.id))

        self.conn.commit()

    def export(self, register_data: data.ContentsData = None):

        sql = 'INSERT INTO contents( ' \
              '  store_label, name, product_number, extension ' \
              '  , tag, publish_date, file_date, file_count ' \
              '  , size, rating, comment, remark ' \
              '  , file_status ' \
              '  , created_at, updated_at) ' \
              ' VALUES(%s, %s, %s, %s ' \
              ', %s, %s, %s, %s ' \
              ', %s, %s, %s, %s ' \
              ', %s ' \
              ', %s, %s)'

        self.cursor.execute(sql, (register_data.storeLabel
                                  , register_data.name, register_data.productNumber, register_data.extension
                                  , register_data.tag, register_data.publishDate, register_data.fileDate
                                  , register_data.fileCount
                                  , register_data.size
                                  , register_data.rating, register_data.comment, register_data.remark
                                  , register_data.fileStatus
                                  , register_data.createdAt, register_data.updatedAt))

        self.conn.commit()
