drop table "oplog";
drop sequence "oplog_seq";

create table "oplog"
(
"_id" varchar2(17) not null,
"ts" number not null,
"v" number(1) not null,
"op" varchar2(10) not null,
"ns" varchar2(100) not null,
"o" varchar2(4000) not null,
"o2" varchar2(4000) null
);

create sequence "oplog_seq";

alter table "oplog" add constraint "oplog_pk" primary key ("_id");

create index "oplog_i1" on "oplog" ("ts");

exit

