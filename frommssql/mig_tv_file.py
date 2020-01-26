from frommssql import mig_base
from frommssql import db
import re


class MigrationTvFile(mig_base.MigrationFromMssqlBase):

	def __init__(self):
		super(MigrationTvFile, self).__init__()
		self.store_dao = db.store.StoreDao()
		self.contents_dao = db.contents.ContentsDao()

	def __get_duration(self, str_duration: str = ''):
		if len(str_duration) <= 0:
			return 0

		int_duration = 0
		# print(str_duration)
		# if re.match(str_duration, '[0-9]{1}' + re.escape(':') + '[0-9]{2}'):
		if re.match('[0-9]{1,2}:[0-9]{1,2}', str_duration):
		# if re.search(str_duration, '[0-9]*'):
			arr_time = str_duration.split(':')
			int_duration = (int(arr_time[0]) * 60) + int(arr_time[1])
		elif re.match('[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}', str_duration):
			arr_time = str_duration.split(':')
			int_duration = (int(arr_time[0]) * 3600) + (int(arr_time[1]) * 60) + int(arr_time[2])

		return int_duration

	# def update_exist_file(self):
	def execute(self):
		self.mssql_cursor.execute('SELECT コンテンツID, 詳細ID, 格納ID, ラベル, '
									'名前, 入手元, 時間, 動画情報, '
									'画質, 動画コメント, 更新日時, サイズ, '
									'優先順位, 備考 '
									'FROM ファイル ')

		row = self.mssql_cursor.fetchone()

		idx = 0
		while row:
			contents_id = row[0]
			detail_id = row[1]
			store_id = row[2]
			label = row[3]
			name = row[4]
			# source = row[5]
			source = 'my'
			duration = self.__get_duration(row[6])
			video_info = row[7]
			quality = row[8]
			comment = row[9]
			file_date = row[10]
			size = row[11]
			priority_num = row[12]
			remark = row[13]

			row = self.mssql_cursor.fetchone()
			idx = idx + 1

			sql = 'INSERT INTO tv.file ( '\
			'contents_id, detail_id, store_id, label '\
			'  , name, source, duration, video_info '\
			'  , quality, comment, file_date, size '\
			'  , priority_num, remark) '\
			'  VALUES(%s, %s, %s, %s '\
			'  , %s, %s, %s, %s '\
			'  , %s, %s, %s, %s '\
			'  , %s, %s)'

			self.mysql_cursor.execute(sql, (contents_id, detail_id, store_id, label,
									name, source, duration, video_info,
									quality, comment, file_date, size,
									priority_num, remark))

			self.mysql_conn.commit()
			# if idx > 5:
			# 	break
		print('export {}件'.format(idx))


if __name__ == '__main__':
	mig_tv_file = MigrationTvFile()
	mig_tv_file.execute()

