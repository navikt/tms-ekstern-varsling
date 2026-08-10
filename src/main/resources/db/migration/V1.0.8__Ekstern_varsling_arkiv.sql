create table ekstern_varsling_arkiv (
    sendingsId text not null primary key,
    ident text not null,
    varselIds jsonb not null,
    varsling jsonb not null,
    opprettet timestamp with time zone not null,
    ferdigstilt timestamp with time zone,
    arkivert timestamp with time zone not null,
    begrunnelse text not null
) with (fillfactor = 100);

create index ekstern_varsling_arkiv_ident on ekstern_varsling_arkiv(ident);
create index ekstern_varsling_arkiv_varselids on ekstern_varsling_arkiv using gin(varselIds);

create index if not exists ekstern_varsling_ferdigstilt on ekstern_varsling(ferdigstilt);
