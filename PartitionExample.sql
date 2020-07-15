
-- create master table
create table payment_master
(
    payment_id integer DEFAULT nextval('public.payment_payment_id_seq'::regclass) NOT NULL,
    customer_id smallint NOT NULL,
    staff_id smallint NOT NULL,
    rental_id integer NOT NULL,
    amount numeric(5,2) NOT NULL,
	payment_date timestamp without time zone NOT NULL
);

-- create function for adding data to/creating children tables
create or replace function paymentMasterTriggerFunction()
returns trigger as $$
declare
	partition_table text;
	partition_year text;
	partition_month text;
begin
	select extract(year from new.payment_date) into partition_year;
	select extract(month from new.payment_date) into partition_month;
	partition_table:= 'payment_' || partition_year || '_' || partition_month;
	
	if not exists(select relname from pg_class where relname=partition_table) then
		execute 'create table ' || partition_table || '(check( extract(year from payment_date)=' || partition_year || 'AND extract(month from payment_date)=' || partition_month || ')) inherits (payment_master);';
	end if;
	
	execute 'insert into ' || partition_table || '(payment_id, customer_id, staff_id, rental_id, amount, payment_date) values (''' || new.payment_id || ''',''' ||new.customer_id || ''',''' || new.staff_id || ''',''' || new.rental_id || ''',''' || new.amount || ''',''' || new.payment_date || ''');';

	return null;
end
$$ language plpgsql;

-- create trigger forwarding insert to master table statement to previous function

create trigger paymentMasterTrigger
before insert on payment_master
for each row execute procedure paymentMasterTriggerFunction();

-- move data
insert into payment_master (customer_id, staff_id, rental_id, amount, payment_date)
select * from payment;

-- drop old table
drop table payment CASCADE;

-- change name of master table & adjust function
alter table payment_master rename to payment

create or replace function paymentMasterTriggerFunction()
returns trigger as $$
declare
	partition_table text;
	partition_year text;
	partition_month text;
begin
	select extract(year from new.payment_date) into partition_year;
	select extract(month from new.payment_date) into partition_month;
	partition_table:= 'payment_' || partition_year || '_' || partition_month;
	
	if not exists(select relname from pg_class where relname=partition_table) then
		execute 'create table ' || partition_table || '(check( extract(year from payment_date)=' || partition_year || 'AND extract(month from payment_date)=' || partition_month || ')) inherits (payment);';
	end if;
	
	execute 'insert into ' || partition_table || '(payment_id, customer_id, staff_id, rental_id, amount, payment_date) values (''' || new.payment_id || ''',''' ||new.customer_id || ''',''' || new.staff_id || ''',''' || new.rental_id || ''',''' || new.amount || ''',''' || new.payment_date || ''');';

	return null;
end
$$ language plpgsql;

