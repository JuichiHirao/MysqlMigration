# -*- coding: utf-8 -*-
from frommssql import mig_base


class Group2StoreOrFav(mig_base.MigrationFromMssqlBase):

    def execute(self):

        self.mssql_cursor.execute('SELECT COUNT(*) FROM MOVIE_GROUP;')

        row = self.mssql_cursor.fetchone()
        cnt_group = row[0]

        self.mssql_cursor.execute('SELECT ID, NAME, LABEL, EXPLANATION, KIND, CREATE_DATE, UPDATE_DATE FROM MOVIE_GROUP;')

        row = self.mssql_cursor.fetchone()

        cnt_group = 0
        cnt_store = 0
        cnt_fav = 0
        idx = 0
        while row:
            id = row[0]
            name = row[1]
            label = row[2]
            print(label)
            explanation = row[3]
            print(explanation)
            kind = row[4]
            created_at = row[5]
            updated_at = row[6]

            row = self.mssql_cursor.fetchone()

            # kind 1 store name1 DVDRIP-XX
            if kind == 1:
                store_label = name
                sql = 'INSERT INTO store(label ' \
                      ', name1, name2, path, remark ' \
                      ', created_at, updated_at) ' \
                      ' VALUES(%s ' \
                      ', %s, %s, %s, %s ' \
                      ', %s, %s)'

                self.mysql_cursor.execute(sql, (store_label, name, '', explanation, ''
                                          , created_at, updated_at))
                cnt_store = cnt_store + 1

            # kind 3 store name1 = m_group.label mywife, HimeMix
            # kind 3 store name2 = m_group.name 001-040, 201207-201212
            # m_site_content
            #   name1 : site_name mywife m_group.label = site_name, m_group.name = parent_path とマッチ
            #   name2 : parent_path 041-080 m_group.label = site_name, m_group.name = parent_path とマッチ
            if kind == 3:
                store_label = label + ' ' + name
                sql = 'INSERT INTO store(label ' \
                      ', name1, name2, path, remark ' \
                      ', created_at, updated_at) ' \
                      ' VALUES(%s ' \
                      ', %s, %s, %s, %s ' \
                      ', %s, %s)'

                self.mysql_cursor.execute(sql, (store_label, label, name, explanation, ''
                                                , created_at, updated_at))
                cnt_store = cnt_store + 1

            if kind == 4:
                print('name [' + str(name) + ']')
                arr_name = '／'.split(name)
                if arr_name is not None and len(arr_name) > 1:
                    name = ','.join(arr_name)
                    fav_label = name
                else:
                    fav_label = name

                if len(label.strip()) > 0:
                    fav_comment = explanation + ' comment : ' + label
                else:
                    fav_comment = explanation.strip()

                sql = 'INSERT INTO fav(label ' \
                      ', name, fav_type, comment, remark ' \
                      ', created_at, updated_at) ' \
                      ' VALUES(%s ' \
                      ', %s, %s, %s, %s ' \
                      ', %s, %s)'

                self.mysql_cursor.execute(sql, (fav_label, name, 1, fav_comment, ''
                                                , created_at, updated_at))

                cnt_fav = cnt_fav + 1

            self.mysql_conn.commit()

            idx = idx + 1

        print('export ' + str(idx) + '件 mssql ' + str(cnt_group) + '件')
        print('  fav ' + str(cnt_fav) + '件  store' + str(cnt_store) + '件')


if __name__ == '__main__':
    group2store = Group2StoreOrFav()
    group2store.execute()
