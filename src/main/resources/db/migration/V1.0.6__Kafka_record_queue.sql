create table status_oppdatert_record_queue
(
    id serial primary key,
    varselId text not null,
    statusnavn text not null,
    statusinnhold text not null,
    createdAt timestamp with time zone
);

create index status_oppdatert_record_queue_record_key on status_oppdatert_record_queue(varselId);
create index status_oppdatert_record_queue_created_at on status_oppdatert_record_queue(createdAt);

create table doknot_stopp_record_queue
(
    id serial primary key,
    sendingsId text not null,
    createdAt timestamp with time zone
);

create index doknot_stopp_record_queue_record_key on doknot_stopp_record_queue(sendingsId);
create index doknot_stopp_record_queue_created_at on doknot_stopp_record_queue(createdAt);
