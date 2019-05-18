# -*- coding: utf-8 -*-
from frommssql import mig_base


class MigrationFile(mig_base.MigrationFromMssqlBase):

    def check(self):
        self.mssql_cursor.execute('SELECT ID ' \
                                  ', NAME, SIZE, FILE_DATE, RATING ' \
                                  ', LABEL, SELL_DATE, COMMENT, REMARK ' \
                                  ', PRODUCT_NUMBER, FILE_COUNT, EXTENSION, TAG ' \
                                  ', CREATE_DATE, UPDATE_DATE FROM MOVIE_FILES')

        row = self.mssql_cursor.fetchone()

        idx = 0
        while row:
            id = row[0]
            kind = 1
            name = row[1].encode('utf-8')
            size = row[2]
            file_date = row[3]
            rating = self.get_column_int(row[4])
            label = row[5].encode('utf-8')
            sell_date = row[6]
            comment = self.get_column_encode(row[7])
            remark = self.get_column_encode(row[8])
            p_number = self.get_column_encode(row[9])
            file_count = row[10]
            extension = row[11]
            tag = self.get_column_encode(row[12])
            created_at = row[13]
            updated_at = row[14]

            row = self.mssql_cursor.fetchone()

    def execute(self):

        self.mssql_cursor.execute('SELECT ID ' \
                                  ', NAME, SIZE, FILE_DATE, RATING ' \
                                  ', LABEL, SELL_DATE, COMMENT, REMARK ' \
                                  ', PRODUCT_NUMBER, FILE_COUNT, EXTENSION, TAG ' \
                                  ', CREATE_DATE, UPDATE_DATE FROM MOVIE_FILES')

        row = self.mssql_cursor.fetchone()

        idx = 0
        while row:
            id = row[0]
            kind = 1
            name = row[1].encode('utf-8')
            size = row[2]
            file_date = row[3]
            rating = self.get_column_int(row[4])
            label = row[5].encode('utf-8')
            sell_date = row[6]
            comment = self.get_column_encode(row[7])
            remark = self.get_column_encode(row[8])
            p_number = self.get_column_encode(row[9])
            file_count = row[10]
            extension = row[11]
            tag = self.get_column_encode(row[12])
            created_at = row[13]
            updated_at = row[14]

            row = self.mssql_cursor.fetchone()

            sql = 'INSERT INTO scraping.contents (kind ' \
                  '  , name, size, file_date, rating ' \
                  '  , label, sell_date, comment, remark ' \
                  '  , product_number, file_count, extension, tag ' \
                  '  , created_at, updated_at) ' \
                  ' VALUES(%s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s)'

            self.mysql_cursor.execute(sql, (1, name, size, file_date, rating
                                            , label, sell_date, comment, remark
                                            , p_number, file_count, extension, tag
                                            , created_at, updated_at))

            self.mysql_conn.commit()

            idx = idx + 1

        print('export ' + str(idx) + '件')


if __name__ == '__main__':
    mig_file = MigrationFile()
    mig_file.execute()
