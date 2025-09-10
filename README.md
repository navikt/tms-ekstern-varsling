# tms-ekstern-varsling
Prosjekt for bestilling av eksterne varsler (SMS og e-post) for brukervarsel-tjenesten.

## Funksjon for app
Appen `tms-ekstern-varsling` har følgende hovedfunksjoner:

- Mottar og lagrer bestillinger om ekstern varsling fra internet topic `min-side.brukervarsel-v1`.
- Oppretter bestilling for utsending av SMS og e-post til eksterne varslingssystemer på topicen `teamdokumenthandtering.privat-dok-notifikasjon`
- Overvåker og oppdaterer status for utsendte bestillinger slik at andre systemer kan reagere på endringer i status.
- Håndterer utsendelser av varsler i batch og bestillinger som skal sendes på et framtidig tidspunkt.
- Håndterer kansellering eller inaktivering av varsler etter behov
  - Ved allerede utført bestilling opprettes event på topic `teamdokumenthandtering.privat-dok-notifikasjon-stopp`

## Ulike eventer appen forholder seg på topic `min-side.brukervarsel-v1`
- `opprett`: Lytter etter nye bestillinger
- `inaktivert`: Lytter for evt. kansellering av bestilling
- `eksternVarslingStatus`: Lytter for status på ekstern bestilling
- `eksternVarslingStatusOppdatert`: Sender status på bestlling

## Dokumentasjon

Mer info om varsler finnes i [dokumentasjonen](https://navikt.github.io/tms-dokumentasjon/varsler/).

## Henvendelser

Spørsmål knyttet til koden eller prosjektet kan stilles som issues her på github.

## For NAV-ansatte

Interne henvendelser kan sendes via Slack i kanalen #min-side
