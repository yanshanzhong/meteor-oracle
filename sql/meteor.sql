
connect meteor/meteor

create table tasks
(
	user_id varchar2(20),
	name varchar2(50),
	description varchar2(250),
	due_date date,
	done varchar2(1)
);

alter table tasks add constraint tasks_pk primary key (user_id, name);

exit

