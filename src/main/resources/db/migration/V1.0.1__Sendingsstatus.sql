alter table ekstern_varsling
    add column status text not null;


alter table ekstern_varsling
    rename column sendt to ferdigstilt;
