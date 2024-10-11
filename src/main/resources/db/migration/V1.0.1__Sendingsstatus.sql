alter table ekstern_varsling
    add column status text;


alter table ekstern_varsling
    rename column sendt to ferdigstilt;
