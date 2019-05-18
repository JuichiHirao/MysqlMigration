# -*- coding: utf-8 -*-
import os
from frommssql import mig_base
from frommssql import db


class MigrationContentsTables(mig_base.MigrationFromMssqlBase):

    def __init__(self):
        super(MigrationContentsTables, self).__init__()
        self.store_dao = db.store.StoreDao()
        self.contents_dao = db.contents.ContentsDao()
        self.stores = self.store_dao.get_all()

        # INSERT INTO MOVIE_GROUP (ID, NAME, LABEL, EXPLANATION, KIND, CREATE_DATE, UPDATE_DATE)
        #   VALUES (818, 'DVDRip10-1', '', '\\TWELVE-SRV\DVDRIP-10\201703', 1, '2017-03-03 21:49:57.613', '2018-08-09 22:26:29.123');
        # INSERT INTO MOVIE_FILES (ID, NAME, SIZE, FILE_DATE, RATING, LABEL, SELL_DATE, COMMENT, REMARK, PRODUCT_NUMBER, FILE_COUNT, EXTENSION, CREATE_DATE, UPDATE_DATE, TAG)
        #   VALUES (82, '[シロウトTV] 初々131 SD るみ 20才 大学生 ◎', 466052875, '2010-11-07 16:21:06.280', 5, '\\twelve-srv\ContentsSeries\MGS - シロウトTV', null, null, null, '', 1, 'WMV', '2012-12-07 13:11:07.343', '2013-01-13 01:28:42.130', null);

    def store_check(self):

        label_list = []
        self.mssql_cursor.execute('SELECT LABEL FROM MOVIE_FILES GROUP BY LABEL ')

        row = self.mssql_cursor.fetchone()

        while row:
            label_list.append(row[0])
            row = self.mssql_cursor.fetchone()

        # self.mssql_conn.close()
        # self.mssql_cursor = self.mssql_conn.cursor()
        '''
        self.mssql_cursor.execute('SELECT LABEL FROM MOVIE_SITECONTENTS GROUP BY LABEL ')

        row = self.mssql_cursor.fetchone()

        while row:
            label_list.append(row[0])
            row = self.mssql_cursor.fetchone()
        '''

        self.mssql_conn.commit()

        for label in label_list:
            store_filter = filter(lambda store: store.path.upper() == label.upper(), self.stores)
            store_list = list(store_filter)

            if len(store_list) < 1:
                print('storeにlabel[' + label + ']がない ' + str(len(store_list)) + '件' )
                # print('store_list [' + str(store_list[0]) + '件' )
                exit(-1)

    def execute_files(self):

        self.mssql_cursor.execute('SELECT ID ' \
                                  ', NAME, SIZE, FILE_DATE, RATING ' \
                                  ', LABEL, SELL_DATE, COMMENT, REMARK ' \
                                  ', PRODUCT_NUMBER, FILE_COUNT, EXTENSION, TAG ' \
                                  ', CREATE_DATE, UPDATE_DATE FROM MOVIE_FILES')

        row = self.mssql_cursor.fetchone()

        idx = 0
        while row:
            # if idx > 100:
            #     break
            label = row[5]
            # id = row[0]
            store_filter = filter(lambda store: store.path.upper() == label.upper(), self.stores)
            store_list = list(store_filter)

            if len(store_list) < 1:
                print('storeにlabel[' + label + ']がない ' + str(len(store_list)) + '件' )
                store_label = ''
                # exit(-1)
            elif len(store_list) >= 1:
                store_label = store_list[0].label

            name = row[1].encode('utf-8')
            size = row[2]
            file_date = row[3]
            rating = self.get_column_int(row[4])
            label = row[5]
            sell_date = row[6]
            comment = self.get_column_encode(row[7])
            remark = self.get_column_encode(row[8])
            p_number = self.get_column_encode(row[9])
            # file_count = row[10]
            extension = row[11]
            tag = self.get_column_encode(row[12])
            created_at = row[13]
            updated_at = row[14]

            row = self.mssql_cursor.fetchone()

            sql = 'INSERT INTO contents (store_label ' \
                  '  , name, product_number, extension, tag ' \
                  '  , publish_date, file_date, file_count, size ' \
                  '  , rating, comment, remark, is_not_exist ' \
                  '  , created_at, updated_at) ' \
                  ' VALUES(%s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s)'

            self.mysql_cursor.execute(sql, (store_label
                                            , name, p_number, extension, tag
                                            , sell_date, file_date, 0, size
                                            , rating, comment, remark, 0
                                            , created_at, updated_at))

            self.mysql_conn.commit()

            idx = idx + 1

        print('export ' + str(idx) + '件')

    def execute_site(self):

        self.mssql_cursor.execute('SELECT ID ' \
                                  ', NAME, MOVIE_NEWDATE, RATING, SITE_NAME ' \
                                  ', LABEL, COMMENT, REMARK, PARENT_PATH ' \
                                  ', MOVIE_COUNT, PHOTO_COUNT, EXTENSION, TAG ' \
                                  ', CREATE_DATE, UPDATE_DATE ' \
                                  ' FROM MOVIE_SITECONTENTS')

        row = self.mssql_cursor.fetchone()

        '''
        1 name            061yumika
        2 movie_newdate   2005-12-24 05:19:26.000
        3 rating          5
        4 site_name       舞ワイフ
        5 label           null nullでないのもあるが、parent_pathに同じの入っているから無視
        6 comment         null
        7 remark          null
        8 parent_path     041-080
        9 movie_count     10/6
        10 photo_count     40
        11 extension       WMV
        12 tag
        '''

        idx = 0
        while row:
            # if idx > 100:
            #     break
            store_label = row[4] + ' ' + row[8]
            store_filter = filter(lambda store: store.label.upper() == store_label.upper(), self.stores)
            store_list = list(store_filter)

            if len(store_list) >= 1:
                print('storeにlabel[' + store_label + ']がない ' + str(len(store_list)) + '件' )
                store_label = store_list[0].label
            else:
                # exit(-1)
                store_label = ''

            name = row[1]
            movie_newdate = row[2]  # file_date
            rating = self.get_column_int(row[3])
            comment = self.get_column_encode(row[6])
            remark = ''
            extension = row[11]
            tag = self.get_column_encode(row[12])
            created_at = row[13]
            updated_at = row[14]

            row = self.mssql_cursor.fetchone()

            sql = 'INSERT INTO contents (store_label ' \
                  '  , name, extension, tag, file_date ' \
                  '  , rating, comment, remark ' \
                  '  , created_at, updated_at) ' \
                  ' VALUES(%s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s ' \
                  ', %s, %s)'

            self.mysql_cursor.execute(sql, (store_label
                                            , name, extension, tag, movie_newdate
                                            , rating, comment, remark
                                            , created_at, updated_at))

            self.mysql_conn.commit()

            idx = idx + 1

        print('export site ' + str(idx) + '件')

    def execute_contents(self):

        '''
        853
        ACTRESS_NAME    楓姫輝／白石里佳
                        小泉ありさ／新庄小雪

        SITE_NAME       DVDRip
                        WOMAN INSIDE

        LINK_PATH       GS-670 歌舞伎町整体治療院 35（ゴーゴーズ）
                        NoFile DMM月-AVステーション【TMA】丸の内美人女子社員生中出し [ID-051 20080206]
                        148koyuki

        CONTENTS_DATE   2009/05/01
                        2009-06-01

        INSERT INTO MOVIE_CONTENTS (ID, ACTRESS_NAME, SITE_NAME, LINK_PATH, CONTENTS_DATE)
          VALUES (21, '＠YOU', 'S-CUTE', 'ps4_93_you2', '2009-11-21');
        '''
        self.mssql_cursor.execute('SELECT ID ' \
                                  '  , ACTRESS_NAME, SITE_NAME, LINK_PATH, CONTENTS_DATE ' \
                                  'FROM MOVIE_CONTENTS ')

        row = self.mssql_cursor.fetchone()

        idx = 0
        row_ok = 0
        row_update = 0
        row_nazo = 0
        while row:
            # if idx > 40:
            #     break

            id = row[0]
            actress_name = row[1]
            site_name = row[2]
            link_path = row[3]
            contents_date = row[4]

            is_not_exist = 0
            if 'NoFile' in link_path:
                is_not_exist = 2
                print('is not exist [' + link_path + ']')
            else:
                arr_link_path = os.path.splitext(link_path)
                link_path_name = arr_link_path[0]
                if len(arr_link_path) > 1:
                    extension = arr_link_path[1].replace('.', '')
                else:
                    extension = ''
                where = 'WHERE name = %s '
                contents_list = self.contents_dao.get_where_agreement(where, (link_path_name, ))

                if contents_list is None:
                    print('nothing data [' + actress_name + ']   ' + link_path_name)
                # 存在しない場合は、is_not_existを9にして、登録
                else:
                    if len(contents_list) > 1:
                        match_extension = list(filter(lambda one_contents: one_contents.extension.upper() == extension.upper(), contents_list))
                        if match_extension is not None and len(match_extension) == 1:
                            match_contents = match_extension[0]
                            row_ok = row_ok + 1
                        else:
                            match_contents = None
                            print('many data [' + actress_name + ']   ' + link_path_name)
                            row_nazo = row_nazo + 1
                    else:
                        match_contents = contents_list[0]

                    if match_contents is not None:
                        # 存在する場合は、タグを付ける
                        is_exist_tag = False

                        if match_contents.tag == actress_name:
                            is_exist_tag = True
                        if actress_name in match_contents.tag:
                            is_exist_tag = True

                        if len(match_contents.tag) > 0:
                            actress_name = match_contents.tag + ',' + actress_name

                        if not is_exist_tag:
                            print('tag change [' + match_contents.tag + '] -> [' + actress_name + ']  ' + link_path)
                            row_update = row_update + 1
                        else:
                            row_ok = row_ok + 1

            row = self.mssql_cursor.fetchone()

            '''
            sql = 'INSERT INTO contents (store_label ' \
                  '  , name, product_number, extension, tag ' \
                  '  , publish_date, file_date, file_count, size ' \
                  '  , rating, comment, remark, is_not_exist ' \
                  '  , created_at, updated_at) ' \
                  ' VALUES(%s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s, %s, %s ' \
                  ', %s, %s)'

            self.mysql_cursor.execute(sql, (store_label
                                            , name, p_number, extension, tag
                                            , sell_date, file_date, 0, size
                                            , rating, comment, remark, 0
                                            , created_at, updated_at))

            self.mysql_conn.commit()
            '''

            idx = idx + 1

        print('export ' + str(idx) + '件')
        print('  OK ' + str(row_ok) + '件')
        print('  update ' + str(row_update) + '件')
        print('  nazo ' + str(row_nazo) + '件')


if __name__ == '__main__':
    mig_contents = MigrationContentsTables()
    # mig_contents.store_check()
    # mig_contents.execute_files()
    # mig_contents.execute_site()
    mig_contents.execute_contents()
