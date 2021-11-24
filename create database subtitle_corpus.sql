DROP TABLE IF EXISTS subtitle_meta;
create table subtitle_meta (
  id_meta int(10) not null PRIMARY key,
  id_movie int(10) not null,
  duration varchar(20),
  translated_language varchar(20),
  original_language varchar(50),
  country varchar(20),
  year int(10),
  genre1 varchar(20),
  genre2 varchar(20),
  genre3 varchar(20)
) engine=InnoDB default charset=utf8;


DROP TABLE IF EXISTS subtitle_token;
create table subtitle_token (
  id_token int(10) not null primary key,
  token varchar(20) not null,
  lemma varchar(20),
  pos varchar(10),
  verb_form varchar(10),
  tense varchar(10)
) engine=InnoDB default charset=utf8;

DROP TABLE IF EXISTS subtitle_join;
create table subtitle_join (
  id_join int(10) not null primary key,
  id_meta int(10) not null,
  id_token int(10) not null,
  FOREIGN KEY (id_meta) REFERENCES subtitle_meta(id_meta),
  FOREIGN KEY (id_token) REFERENCES subtitle_token(id_token)
) engine=InnoDB default charset=utf8;
