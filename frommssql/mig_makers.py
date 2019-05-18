# -*- coding: utf-8 -*-
from frommssql import mig_base


class MigrationMaker(mig_base.MigrationFromMssqlBase):

    def execute(self):

        self.mssql_cursor.execute('SELECT ID, NAME, LABEL, KIND, MATCH_STR, MATCH_PRODUCT_NUMBER, CREATE_DATE, UPDATE_DATE FROM MOVIE_MAKERS;')

        row = self.mssql_cursor.fetchone()

        idx = 0
        while row:
            id = row[0]
            name = row[1].encode('utf-8')
            label = row[2].encode('utf-8')
            kind = row[3]
            match_str = row[4].encode('utf-8')
            if row[5] is None:
                match_product_number = ''
            else:
                match_product_number = row[5].encode('utf-8')
            created_at = row[6]
            updated_at = row[7]
            # print ('[' + str(id) + ']')
            # print ('  NAME : LABEL [' + name + ' : ' + label + ']')
            # print ('  KIND  [' + str(kind) + ']   MATCH_STR [' + match_str + ']')
            # print ('  MATCH_PRODUCT_NUMBER [' + match_product_number + ']')
            # print ('  [' + str(created_at) + ']   [' + str(updated_at) + ']')
            row = self.mssql_cursor.fetchone()

            sql = 'INSERT INTO scraping.movie_makers (name, label' \
                        ', kind, match_str, match_product_number' \
                        ', created_at, updated_at) ' \
                        ' VALUES(%s, %s, %s, %s, %s, %s, %s)'

            self.mysql_cursor.execute(sql, (name, label
                                    , kind, match_str, match_product_number
                                    , created_at, updated_at))

            self.mysql_conn.commit()

            idx = idx + 1

        print('export ' + str(idx) + '件')

