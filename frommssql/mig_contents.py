# -*- coding: utf-8 -*-
import os
import re
from frommssql import mig_base
from frommssql import db
from frommssql import data


class MigrationContentsTables(mig_base.MigrationFromMssqlBase):

    def __init__(self):
        super(MigrationContentsTables, self).__init__()
        self.store_dao = db.store.StoreDao()
        self.contents_dao = db.contents.ContentsDao()
        self.stores = self.store_dao.get_all()

    def store_check(self):

        label_list = []
        self.mssql_cursor.execute('SELECT '
                                  'コンテンツID, 詳細ID, 格納ID, ラベル, '
                                  '名前, 入手元, 時間, 動画情報, '
                                  '画質, 動画コメント, 更新日時, サイズ, '
                                  '優先順位, 備考 '
                                  'FROM ファイル ')

        row = self.mssql_cursor.fetchone()

        while row:
            label_list.append(row[0])
            row = self.mssql_cursor.fetchone()

        # self.mssql_conn.close()
        # self.mssql_cursor = self.mssql_conn.cursor()
        self.mssql_cursor.execute('SELECT LABEL FROM MOVIE_SITECONTENTS GROUP BY LABEL ')

        row = self.mssql_cursor.fetchone()

        while row:
            label_list.append(row[0])
            row = self.mssql_cursor.fetchone()

        self.mssql_conn.commit()

        for label in label_list:
            # print(label.upper())
            store_filter = filter(lambda store: store.path.upper() == label.upper(), self.stores)
            # print('AFTER' + label.upper())
            store_list = list(store_filter)

            if len(store_list) < 1:
                print('storeにlabel[' + label + ']がない ' + str(len(store_list)) + '件' )
                # print('store_list [' + str(store_list[0]) + '件' )
                # exit(-1)

    def execute_files(self):

        self.mssql_cursor.execute('SELECT ID '
                                  ', NAME, SIZE, FILE_DATE, RATING '
                                  ', LABEL, SELL_DATE, COMMENT, REMARK '
                                  ', PRODUCT_NUMBER, FILE_COUNT, EXTENSION, TAG '
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
                print('storeにlabel[' + label + ']がない ' + str(len(store_list)) + '件 ID[' + str(row[0]) + ']')
                store_label = ''
                # exit(-1)
            elif len(store_list) >= 1:
                store_label = store_list[0].label

            register_data = data.ContentsData()
            register_data.storeLabel = store_label
            register_data.name = row[1].encode('utf-8')
            register_data.size = row[2]
            register_data.fileDate = row[3]
            register_data.rating = self.get_column_int(row[4])
            register_data.label = row[5]
            register_data.publishDate = row[6]
            register_data.comment = self.get_column_encode(row[7])
            register_data.remark = self.get_column_encode(row[8])
            register_data.productNumber = self.get_column_encode(row[9])
            register_data.fileCount = row[10]
            register_data.extension = row[11]
            register_data.tag = self.get_column_encode(row[12])
            register_data.fileStatus = 'exist'
            register_data.createdAt = row[13]
            register_data.updatedAt = row[14]

            row = self.mssql_cursor.fetchone()

            self.contents_dao.export(register_data)

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

            if row[4] == 'Tokyo247':
                if 'COLLECTION' in row[1]:
                    store_label = row[4] + ' COLLECTION'
                elif 'Maxi-247' in row[1]:
                    store_label = row[4] + ' Max247'
                else:
                    store_label = row[4] + ' \\\\TWELVE-SRV\ContentsSeries3\TOKYO247'

            if store_label == 'RealShodo ':
                store_label = row[4] + ' \\\\twelve-srv\ContentsSeries\RealShodo'

            if store_label == '人妻パラダイス ':
                store_label = row[4] + ' \\\\twelve-srv\ContentsSeries3\人妻パラダイス'
            elif re.search('人妻パラダイス [0-2].*', store_label):
                store_label = row[4] + ' _MAIN\\' + row[8]

            if store_label == 'WOMAN INSIDE ':
                store_label = row[4] + ' \\\\twelve-srv\ContentsSeries2\WOMAN INSIDE'

            if store_label == 'S-CUTE ':
                file_status = 'site not exist'
            else:
                file_status = 'exist'

            store_filter = filter(lambda store: store.label.upper() == store_label.upper(), self.stores)
            store_list = list(store_filter)

            if len(store_list) >= 1:
                store_label = store_list[0].label
            else:
                print('storeにlabel[' + store_label + ']がない ' + str(len(store_list)) + '件 ID [' + str(row[0]) + '] ' + row[1] + ' ' + file_status)
                store_label = ''
                # exit(-1)

            register_data = data.ContentsData()
            register_data.storeLabel = store_label
            register_data.name = row[1]
            # movie_newdate = row[2]  # file_date
            register_data.fileDate = row[2]
            register_data.rating = self.get_column_int(row[3])
            register_data.comment = self.get_column_encode(row[6])
            register_data.remark = ''
            register_data.extension = row[11]
            register_data.tag = self.get_column_encode(row[12])
            register_data.createdAt = row[13]
            register_data.updatedAt = row[14]

            row = self.mssql_cursor.fetchone()

            self.contents_dao.export(register_data)

            idx = idx + 1

        print('export site ' + str(idx) + '件')

    def execute_contents(self):

        """
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
        """

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

            register_data = data.ContentsData()
            # register_data.id = row[0]
            actress_name = row[1]
            site_name = row[2]
            link_path = row[3]
            contents_date = row[4]

            register_data.fileStatus = 'exist'
            if 'NoFile' in link_path:
                register_data.fileStatus = 'no file'
                print('file_status is not exist [' + link_path + ']')
            else:
                arr_link_path = os.path.splitext(link_path)
                link_path_name = arr_link_path[0]
                if len(arr_link_path) > 1:
                    register_data.extension = arr_link_path[1].replace('.', '')
                else:
                    register_data.extension = ''
                where = 'WHERE name = %s '
                contents_list = self.contents_dao.get_where_agreement(where, (link_path_name, ))

                register_data.name = link_path
                if contents_list is None:
                    print('nothing data [' + actress_name + ']   ' + link_path_name)
                    # 存在しない場合は、file_statusを'nothing data'にして、登録
                    register_data.fileStatus = 'nothing data'
                    register_data.tag = actress_name
                    register_data.name = link_path
                    register_data.fileDate = contents_date
                    register_data.remark = site_name
                    self.contents_dao.export(register_data)
                else:
                    if len(contents_list) > 1:
                        match_extension = list(filter(lambda one_contents:
                                                      one_contents.extension.upper() == register_data.extension.upper()
                                                      , contents_list))
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

                        register_data.tag = actress_name

                        if not is_exist_tag:
                            register_data.id = match_contents.id
                            self.contents_dao.update_tag(register_data)
                            print('tag change [' + match_contents.tag + '] -> [' + actress_name + ']  ' + link_path)
                            row_update = row_update + 1
                        else:
                            row_ok = row_ok + 1
                    else:
                        print('None match_contents ' + link_path)
                        row_nazo = row_nazo + 1

            row = self.mssql_cursor.fetchone()

            idx = idx + 1

        print('export ' + str(idx) + '件')
        print('  OK ' + str(row_ok) + '件 既にtag設定済み')
        print('  update ' + str(row_update) + '件')
        print('  nazo ' + str(row_nazo) + '件')


if __name__ == '__main__':
    mig_contents = MigrationContentsTables()
    # mig_contents.store_check()
    # mig_contents.execute_files()
    # mig_contents.execute_site()
    mig_contents.execute_contents()
