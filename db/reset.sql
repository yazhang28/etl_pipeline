begin transaction;
delete from users;
delete from test;
delete from process;
commit;
vacuum;
