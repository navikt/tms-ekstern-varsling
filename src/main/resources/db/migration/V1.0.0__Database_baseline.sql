create table eksterne_varsler(
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


create index eksterne_varsler_ident on eksterne_varsler(ident);
create index eksterne_varsler_fremtidig_utsending on eksterne_varsler(utsending)
    where sendt is null;

create index eksterne_varsler_varsler on eksterne_varsler using gin(varsler);