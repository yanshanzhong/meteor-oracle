
accept SID char default XE prompt 'Enter SID [XE]: '
accept system_password char default manager prompt 'Enter SYSTEM password: ' hide

prompt Connecting as SYSTEM
connect system/&&system_password@&&SID

create user meteor identified by meteor;

exit

