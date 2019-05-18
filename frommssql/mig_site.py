# -*- coding: utf-8 -*-
from frommssql import mig_base


class MigrationFile(mig_base.MigrationFromMssqlBase):

    def execute(self):

        self.mssql_cursor.execute('SELECT ID ' \
                                  ', NAME, MOVIE_NEWDATE, RATING, SITE_NAME ' \
                                  ', LABEL, COMMENT, REMARK, PARENT_PATH ' \
                                  ', MOVIE_COUNT, PHOTO_COUNT, EXTENSION, TAG ' \
                                  ', CREATE_DATE, UPDATE_DATE FROM MOVIE_SITECONTENTS')

        row = self.mssql_cursor.fetchone()

        idx = 0
        while row:
            id = row[0]
            kind = 2
            name = row[1].encode('utf-8')
            size = 0
            file_date = row[2]
            rating = row[3]
            label = row[4].encode('utf-8') # SITE_NAME
            # label = row[5].encode('utf-8') # LABEL
            comment = row[6].encode('utf-8')
            remark = row[7].encode('utf-8')
            parent_path = row[8].encode('utf-8')
            file_count = row[9]
            photo_count = row[10]
            extension = row[11]
            tag = row[12].encode('utf-8')
            created_at = row[13]
            updated_at = row[14]

            row = self.mssql_cursor.fetchone()

            sql = 'INSERT INTO scraping.contents (kind, ' \
                  '  , name, size, file_date, rating ' \
                  '  , label, sell_date, comment, remark ' \
                  '  , product_number, file_count, extension, tag ' \
                  ', created_at, updated_at) ' \
                  ' VALUES(%s, ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s)'

            self.mysql_cursor.execute(sql, (kind, name, size, file_date, rating
                                            , label, sell_date, comment, remark
                                            , p_number, file_count, extension, tag
                                            , created_at, updated_at))

            self.mysql_conn.commit()

            idx = idx + 1

        print('export ' + str(idx) + '件')
