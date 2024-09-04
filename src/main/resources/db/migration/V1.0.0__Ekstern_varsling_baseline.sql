create table ekstern_varsling(
                                 sendingsId text primary key,
                                 ident text not null,
                                 erBatch bool not null,
                                 erUtsattVarsel bool not null,
                                 varsler jsonb not null,
                                 utsending timestamp with time zone,
                                 kanal text not null,
                                 sendt timestamp with time zone,
                                 opprettet timestamp with time zone not null
);


create index ekstern_varsling_ident on ekstern_varsling(ident);
create index ekstern_varsling_fremtidig_utsending on ekstern_varsling(utsending)
    where sendt is null;

create index ekstern_varsling_varsler on ekstern_varsling using gin(varsler);
