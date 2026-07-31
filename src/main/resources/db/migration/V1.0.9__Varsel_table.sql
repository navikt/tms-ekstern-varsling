create table varsel(
    varselId text primary key,
    sendingsId text not null references ekstern_varsling(sendingsId),
    varseltype text not null,
    preferertKanal text,
    smsVarslingstekst text,
    epostVarslingstittel text,
    epostVarslingstekst text,
    aktiv bool not null,
    produsent jsonb not null,
    opprettet timestamp with time zone not null,
    inaktivert timestamp with time zone
);
