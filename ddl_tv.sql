			コンテンツID int,
			詳細ID int, 格納ID int,
			ラベル varchar(50), 名前 varchar(1024),
			入手元 int, 時間 varchar(50),
			動画情報 varchar(512), 画質 int,
			動画コメント varchar(2048), 更新日時 datetime,
			サイズ decimal(18), 優先順位 int,
			備考 varchar(2048)

		"""
		"""
drop table tv.file;
create table tv.file
(
    id mediumint auto_increment primary key,
    contents_id int,
    detail_id int,
    store_id int,
    label varchar(255),
    name text,
    source ENUM('my', 'net'),
    duration int,
    video_info varchar(255),
    comment text,
    size bigint,
    priority_num int,
    file_date datetime,
    quality tinyint,
    remark text,
    created_at timestamp default CURRENT_TIMESTAMP null,
    updated_at timestamp null on update CURRENT_TIMESTAMP
);

drop table tv.real_file;
create table tv.real_file
(
    id mediumint auto_increment primary key,
    name text,
    path text,
    extension varchar(128),
    ctime datetime,
    mtime datetime,
    atime datetime,
    size bigint,
    remark text,
    created_at timestamp default CURRENT_TIMESTAMP null,
    updated_at timestamp null on update CURRENT_TIMESTAMP
);

drop table tv.real_dir;
create table tv.real_dir
(
    id mediumint auto_increment primary key,
    name text,
    path text,
    ctime datetime,
    mtime datetime,
    atime datetime,
    remark text,
    created_at timestamp default CURRENT_TIMESTAMP null,
    updated_at timestamp null on update CURRENT_TIMESTAMP
);
