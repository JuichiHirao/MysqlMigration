# -*- coding: utf-8 -*-
from frommssql import mig_base
from frommssql import data
from frommssql import db


class Group2StoreOrFav(mig_base.MigrationFromMssqlBase):

    def execute(self):

        self.fav_dao = db.fav.FavDao()
        self.store_dao = db.store.StoreDao()

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
            # print(label)
            explanation = row[3]
            # print(explanation)
            kind = row[4]
            created_at = row[5]
            updated_at = row[6]

            row = self.mssql_cursor.fetchone()

            if kind == 1 or kind == 3:
                stores = self.store_dao.get_where('WHERE label = %s', (label + ' ' + name, ))

                if stores is not None and len(stores) >= 1:
                    print('exist store [' + name + ']')
                    continue

            # kind 1 store name1 DVDRIP-XX
            if kind == 1:

                store = data.StoreData()

                store.label = name
                store.name1 = name
                store.name2 = ''
                store.type = 'file'
                store.path = explanation
                store.remark = ''
                store.createdAt = created_at
                store.updatedAt = updated_at

                self.store_dao.export(store)
                cnt_store = cnt_store + 1

            # kind 3 store name1 = m_group.label mywife, HimeMix
            # kind 3 store name2 = m_group.name 001-040, 201207-201212
            # m_site_content
            #   name1 : site_name mywife m_group.label = site_name, m_group.name = parent_path とマッチ
            #   name2 : parent_path 041-080 m_group.label = site_name, m_group.name = parent_path とマッチ
            if kind == 3:
                store = data.StoreData()

                store.label = label + ' ' + name
                store.name1 = label
                store.name2 = name
                store.type = 'site'
                store.path = explanation
                store.remark = ''
                store.createdAt = created_at
                store.updatedAt = updated_at

                self.store_dao.export(store)
                cnt_store = cnt_store + 1

            if kind == 4:
                arr_name = '／'.split(name)
                if arr_name is not None and len(arr_name) > 1:
                    name = ','.join(arr_name)
                    fav_label = name
                else:
                    fav_label = name

                favs = self.fav_dao.get_where('WHERE label = %s', (fav_label, ))

                if favs is not None and len(favs) >= 1:
                    print('exist fav [' + name + ']')
                    continue

                fav = data.FavData()

                fav.label = fav_label
                fav.name = fav_label
                fav.type = 'actress'
                fav.createdAt = created_at
                fav.updatedAt = updated_at

                if len(label.strip()) > 0:
                    fav.comment = explanation + ' comment : ' + label
                else:
                    fav.comment = explanation.strip()

                self.fav_dao.export(fav)

                cnt_fav = cnt_fav + 1

            idx = idx + 1

        print('export ' + str(idx) + '件 mssql ' + str(cnt_group) + '件')
        print('  fav ' + str(cnt_fav) + '件  store' + str(cnt_store) + '件')


if __name__ == '__main__':
    group2store = Group2StoreOrFav()
    group2store.execute()
