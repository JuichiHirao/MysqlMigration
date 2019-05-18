# -*- coding: utf-8 -*-
from frommssql import mig_base


class MigrationImport(mig_base.MigrationFromMssqlBase):

    def execute(self):

        self.mssql_cursor.execute('SELECT ID, COPY_TEXT, KIND, MATCH_PRODUCT, PRODUCT_NUMBER, PRODUCT_DATE, MAKER, TITLE, ACTRESSES, RAR_FLAG, TAG, FILENAME, HD_KIND, MOVIE_FILES_ID, SPLIT_FLAG, NAME_ONLY_FLAG, CREATE_DATE, UPDATE_DATE FROM MOVIE_IMPORT;')

        row = self.mssql_cursor.fetchone()

        idx = 0
        while row:
            id = row[0]
            copy_text = row[1].encode('utf-8')
            kind = row[2]
            match_product = row[3].encode('utf-8')
            product_number = row[4].encode('utf-8')
            product_date = row[5]
            maker = row[6].encode('utf-8')
            title = row[7].encode('utf-8')
            actress = row[8].encode('utf-8')
            rar_flag = row[9]
            tag = row[10].encode('utf-8')
            filename = row[11].encode('utf-8')
            hd_kind = row[12]
            movie_files_id = row[13]
            split_flag = row[14]
            name_only_flag = row[15]
            created_at = row[16]
            updated_at = row[17]

            row = self.mssql_cursor.fetchone()

            sql = 'INSERT INTO scraping.import (copy_text, kind ' \
                  ', match_product, product_number, sell_date, maker ' \
                  ', title, actresses, rar_flag, tag ' \
                  ', filename, hd_kind, movie_file_id, split_flag ' \
                  ', name_only_flag ' \
                  ', created_at, updated_at) ' \
                  ' VALUES(%s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s ' \
                  ', %s, %s)'

            self.mysql_cursor.execute(sql, (copy_text, kind
                                       , match_product, product_number, product_date, maker
                                       , title, actress, rar_flag, tag
                                       , filename, hd_kind, movie_files_id, split_flag
                                       , name_only_flag
                                       , created_at, updated_at))

            self.mysql_conn.commit()

            idx = idx + 1

        print('export ' + str(idx) + '件')
