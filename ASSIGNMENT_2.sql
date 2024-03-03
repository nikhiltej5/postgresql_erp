drop table if exists department cascade;
drop table if exists student cascade;
drop table if exists courses cascade;
drop table if exists professor cascade;
drop table if exists course_offers cascade;
drop table if exists student_courses cascade;
drop table if exists valid_entry cascade;
drop table if exists student_dept_change cascade;

create or replace function check_course() returns trigger as $$
begin 
    if new.dept_id <> substring(new.course_id, 1, 3) then
        raise exception 'Invalid';
    end if;
    return new;
end;
$$ language plpgsql;

create table department (
    dept_id char(3),
    dept_name varchar(40) not null,
    primary key (dept_id),
    unique (dept_name)
);

create table valid_entry (
    dept_id char(3),
    entry_year integer not null,
    seq_number integer not null,
    foreign key (dept_id) references department(dept_id) on update cascade on delete cascade
);

create table professor (
    professor_id varchar(10),
    professor_first_name varchar(40) not null,
    professor_last_name varchar(40) not null,
    office_number varchar(20),
    contact_number char(10) not null,
    start_year integer,
    resign_year integer,
    dept_id char(3),
    primary key (professor_id),
    check (start_year <= resign_year),
    foreign key (dept_id) references department(dept_id) on update cascade on delete cascade
);

create table courses (
    course_id char(6) not null,
    course_name varchar(20) not null,
    course_desc text,
    credits numeric not null,
    dept_id char(3),
    primary key (course_id),
    unique (course_name),
    check (credits > 0),
	foreign key (dept_id) references department(dept_id) on update cascade on delete cascade
);

create trigger check_course
    before insert on courses
    for each row
    execute function check_course();

create table course_offers (
    course_id char(6),
    session varchar(9),
    semester integer not null,
    professor_id varchar(10),
    capacity integer,
    enrollments integer,
    primary key (course_id, session, semester),
    check (semester = 1 or semester = 2),
    foreign key (course_id) references courses(course_id) on update cascade on delete cascade,
    foreign key (professor_id) references professor(professor_id) on update cascade on delete cascade
);

create table student (
    first_name varchar(40) not null,
    last_name varchar(40),
    student_id char(11) not null,
    address varchar(100),
    contact_number char(10) not null,
    email_id varchar(50),
    tot_credits integer not null,
    dept_id char(3),
    primary key (student_id),
    unique (contact_number),
    unique (email_id),
    check (tot_credits >= 0),
    check(char_length(student_id) = 10),
    foreign key (dept_id) references department(dept_id) on update cascade on delete cascade
);

create table student_courses (
    student_id char(11),
    course_id char(6),
    session varchar(9),
    semester integer,
    grade numeric not null,
    check (grade >= 0 and grade <= 10),
    check (semester = 1 or semester = 2),
    foreign key (student_id) references student(student_id) on update cascade on delete cascade,
    foreign key (course_id,session,semester) references course_offers(course_id,session,semester) on delete cascade on update cascade
);

create or replace function validate_student_entry() returns trigger as $$
declare
    dept_id_part char(3);
    entry_year_part integer;
    seq_number_part integer;
    count integer;
    dept_id_part_1 char(3);
    symbol char(1);
    email_domain varchar(20);
    student_id_part varchar(11);
begin
    if new.email_id is null then
        raise exception 'Invalid';
    end if;
    dept_id_part := substring(new.student_id, 5, 3);
    entry_year_part := cast(substring(new.student_id, 1, 4) as integer);
    seq_number_part := cast(substring(new.student_id, 8, 3) as integer);
    dept_id_part_1 := substring(new.email_id, 12, 3);
    symbol := substring(new.email_id, 11, 1);
    email_domain := substring(new.email_id, 15,11);
    student_id_part := substring(new.email_id, 1, 10);
    select count(*) into count from valid_entry where dept_id = dept_id_part and entry_year = entry_year_part and seq_number = seq_number_part;
    if count <> 1 or student_id_part <> new.student_id  or dept_id_part_1 <> dept_id_part or symbol <> '@' or email_domain <> '.iitd.ac.in' then
        raise exception 'Invalid';
    end if;
    update valid_entry set seq_number = seq_number + 1 where dept_id = dept_id_part and entry_year = entry_year_part;
    return new;
end;
$$ language plpgsql;


create trigger validate_student_id
    before insert on student
    for each row
    execute function validate_student_entry();

create or replace function update_seq_num() returns trigger as $$
declare
    dept_id_part char(3);
    entry_year_part integer;
    seq_number_part integer;
begin
    dept_id_part := substring(new.student_id, 5, 3);    
    entry_year_part := cast(substring(new.student_id, 1, 4) as integer);
    seq_number_part := cast(substring(new.student_id, 8, 3) as integer);
    update valid_entry set seq_number = seq_number where dept_id = dept_id_part and entry_year = entry_year_part;
    return new;
end;
$$ language plpgsql;

create trigger update_seq_number
    after insert on student
    for each row
    execute function update_seq_num();

create table if not exists student_dept_change (
    old_student_id char(11) not null,
    old_dept_id char(3) not null,
    new_student_id char(11) not null,
    new_dept_id char(3) not null,
    foreign key (old_dept_id) references department(dept_id) on update cascade,
    foreign key (new_dept_id) references department(dept_id) on update cascade
);

create or replace function validate_student_dept_change() returns trigger as $$
declare
    count1 integer;
    count2 integer;
    avg_grade numeric;
    entry_year1 integer;
    new_student_id1 char(11);
    new_email_id varchar(50);
begin
    entry_year1 := cast(substring(old.student_id, 1, 4) as integer);
    select count(*) into count1 from department where dept_id = old.dept_id;
    if (old.dept_id != new.dept_id and count1 <> 0) then
        select count(*) into count2 from student_dept_change where new_student_id = old.student_id and new_dept_id = old.dept_id;
        if count2 > 0 then 
            raise exception 'Department can be changed only once';
        end if;
        if entry_year1 < 2022 then 
            raise exception 'Entry year must be >= 2022';
        end if;
        select avg(case when grade is not null then grade else 0 end) into avg_grade from student_courses where student_id = old.student_id group by student_id;
        if avg_grade <= 8.5 then
            raise exception 'Low grade';
        end if;

        select seq_number into count2 from valid_entry where dept_id = new.dept_id and entry_year1 = entry_year;
        new_student_id1 := cast(entry_year1 as varchar(4))||new.dept_id||LPAD(cast(count2 as varchar(3)), 3, '0');
        new_email_id := new_student_id1 || '@' || new.dept_id ||'.iitd.ac.in';

        insert into student_dept_change values (old.student_id, old.dept_id, new_student_id1, new.dept_id);
        update valid_entry set seq_number = seq_number + 1 where dept_id = new.dept_id and entry_year1 = entry_year;
        new.student_id := new_student_id1;
        new.email_id := new_email_id;
    end if;
    return new;
end;
$$ language plpgsql;

create trigger log_student_dept_change
    before update on student
    for each row
    execute function validate_student_dept_change();

create materialized view  course_eval as 
select course_id, session, semester,count(student_id) as number_of_students, avg(grade) as average_grade,max(grade) as max_grade, min(grade) as min_grade
from student_courses
group by course_id, session, semester;

create or replace function update_course_eval() returns trigger as $$
begin
    refresh materialized view course_eval;
    if TG_OP = 'DELETE' then
        return old;
    end if;
    return new;
end;
$$ language plpgsql;

create trigger update_course_eval
    after insert or delete or update on student_courses
    for each row
    execute function update_course_eval();

create or replace function update_total_credits() returns trigger as $$
declare
    course_credits integer;
begin 
    select credits into course_credits from courses where courses.course_id = course_id;
    update student set tot_credits = tot_credits + course_credits where student.student_id = new.student_id;
    return NEW;
end;
$$ language plpgsql;

create trigger update_student_credits
    after insert on student_courses
    for each row
    execute function update_total_credits();

create or replace function check_enrollments_limit() returns trigger as $$
declare
    enrollments_part integer;
    capacity_part integer;
    entry_year_part integer;
    course_year integer;
    course_credits integer;
begin

    entry_year_part := cast(substring(new.student_id, 1, 4) as integer);
    course_year := cast(substring(new.session,1,4) as integer);
    select credits into course_credits from courses where courses.course_id = new.course_id;

    select tot_credits into capacity_part
    from student 
    where student_id = new.student_id;

    capacity_part := capacity_part + course_credits;

    select count(*) into enrollments_part 
    from student_courses 
    where student_id = new.student_id and session = new.session and semester = new.semester
    group by student_id;

    enrollments_part := enrollments_part + 1;

    if (course_credits = 5 and entry_year_part <> course_year) then
        raise exception 'Invalid';
    end if;

    if (enrollments_part > 5 or capacity_part > 60) then
        raise exception 'Invalid';
    end if;
    return NEW;
end;
$$ language plpgsql;

create trigger validate_enrollments
    before insert on student_courses
    for each row
    execute function check_enrollments_limit();

create materialized view student_semester_summary as
select student_id, session, semester,
sum(case when s.grade >= 5.0 then s.grade*c.credits else 0 end)/sum(case when s.grade >= 5.0 then c.credits else 0 end) as sgpa,
sum(case when s.grade >= 5.0 then c.credits else 0 end) as credits 
from student_courses as s
join courses as c
on s.course_id = c.course_id
group by student_id, session, semester;

create or replace function refresh_summary() returns trigger as $$
begin
    refresh materialized view student_semester_summary;
    return new;
end;
$$ language plpgsql;

create trigger refresh_summary
    after insert on student_courses
    for each row
    execute function refresh_summary();

create or replace function update_student_credits_on_before_insert() returns trigger as $$
declare
    total_credits integer;
    new_credits integer;
begin 
    select credits into new_credits from courses where courses.course_id = new.course_id;
    select sum(c.credits) into total_credits 
    from student_courses as s 
    join courses as c on s.course_id = c.course_id 
    where s.student_id = new.student_id and s.session = new.session and s.semester = new.semester
    group by s.student_id, s.session, s.semester;
    total_credits := total_credits + new_credits;
    if total_credits > 26 then
        raise exception 'Invalid';
    end if;
    if new.grade < 5.0 then
        raise exception 'Invalid';
    end if;
    return new;
end;
$$ language plpgsql;

create trigger update_student_credits_on_insert
    before insert on student_courses
    for each row
    execute function update_student_credits_on_before_insert();

create or replace function update_student_credits_on_delete() returns trigger as $$
declare
    total_credits integer;
    old_credits integer;
begin
    select credits into old_credits from courses where courses.course_id = old.course_id;
    select tot_credits into total_credits 
    from student as s 
    where s.student_id = student_id;
    total_credits := total_credits - old_credits;
    if total_credits >= 0 then
        update student set tot_credits = total_credits where student_id = old.student_id;
    end if;
    refresh materialized view student_semester_summary;
    return OLD;
end;
$$ language plpgsql;

create trigger update_student_credits_on_delete
    after delete on student_courses
    for each row
    execute function update_student_credits_on_delete();

create or replace function update_student_credits_on_update() returns trigger as $$
declare 
    new_credits integer;
    old_credits integer;
begin
    if (old.course_id = new.course_id) then
        new_credits := (select credits from courses where course_id = new.course_id);
        old_credits := (select credits from courses where course_id = old.course_id);
        update student set tot_credits = tot_credits - old_credits + new_credits where student_id = new.student_id;
        refresh materialized view student_semester_summary;
    end if;
    return NEW;
end;
$$ language plpgsql;

create trigger update_student_credits_on_update
    after update on student_courses
    for each row
    execute function update_student_credits_on_update();

create or replace function check_on_student_courses() returns trigger as $$
declare 
    count integer;
    capacity_part integer;
begin 
    select enrollments into count from course_offers where course_id = new.course_id and session = new.session and semester = new.semester;
    select capacity into capacity_part from course_offers where course_id = new.course_id and session = new.session and semester = new.semester;
    if count >= capacity_part then
        raise exception 'course is full';
    end if;
    update course_offers set enrollments = enrollments + 1 where course_id = new.course_id and session = new.session and semester = new.semester;
    return new;
end;
$$ language plpgsql;

create trigger check_on_student_courses
    before insert on student_courses
    for each row
    execute function check_on_student_courses();

create or replace function add_course() returns trigger as $$
declare
    count_course integer;
    count_prof integer;
    count_teaches integer;
    resign_year_part integer;
    session_year_part integer;
begin
    session_year_part := cast(substring(new.session, 1, 4) as integer);
    select count(*) into count_course from courses where course_id = new.course_id;
    select count(*) into count_prof from professor where professor_id = new.professor_id;
    select count(course_id) into count_teaches from course_offers where course_id = new.course_id and professor_id = new.professor_id;
    if count_teaches >= 5 then
        raise exception 'Invalid';
    end if;
    if count_course <> 0 and count_prof <> 0 then
        select resign_year into resign_year_part from professor where professor_id = new.professor_id;
        if (resign_year_part is null or resign_year_part > session_year_part) then
            return new;
        else 
            raise exception 'Invalid';
        end if;	
    else 
        raise exception 'Invalid';
    end if;
    return new;
end;
$$ language plpgsql;

create trigger add_course
    before insert on course_offers
    for each row
    execute function add_course();

--2.4--continues
create or replace function update_department() returns trigger as $$
begin
    if new.dept_id = old.dept_id then
        return new;
    end if;
    update courses set course_id = new.dept_id||substring(course_id, 4) where dept_id = old.dept_id;
    update student set student_id = substring(student_id, 1, 4)||new.dept_id||substring(student_id, 8) where dept_id = old.dept_id;
    update student set email_id = student_id||'@'||new.dept_id||'.iitd.ac.in' where dept_id = old.dept_id;
    return new;
end;
$$ language plpgsql;

create or replace function delete_department() returns trigger as $$
declare 
    count1 integer;
begin
    select count(*) into count1 from student where dept_id = old.dept_id;
    if count1 <> 0 then
        raise exception 'Invalid';
    end if;
    return old;
end;
$$ language plpgsql;

create trigger update_department
    before update on department
    for each row
    execute function update_department();

create trigger delete_department
    after delete on department
    for each row
    execute function delete_department();