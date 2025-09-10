# tms-ekstern-varsling
Prosjekt for bestilling av eksterne varsler (SMS og e-post) for brukervarsel-tjenesten.

## Hovedfunksjoner

- Mottar og lagrer bestillinger om ekstern varsling fra internt topic `min-side.brukervarsel-v1`.
- Oppretter bestilling for utsending av SMS og e-post til eksterne varslingssystemer på topicen `teamdokumenthandtering.privat-dok-notifikasjon`.
- Overvåker og oppdaterer status på bestillinger slik at andre systemer kan reagere på endringer i status.
- Håndterer utsendelser av varsler i batch og bestillinger som skal sendes på et framtidig tidspunkt.
- Håndterer avbestilling av SMS og e-post ved inaktivering av varsel.

## Eventtyper appen håndterer på topic `min-side.brukervarsel-v1`.
- **opprett**: Lytter etter nye bestillinger om ekstern varsling.
- **inaktivert**: Lytter for eventuelle kanselleringer av bestilling.
- **eksternVarslingStatus**: Lytter for status på ekstern bestilling.
- **eksternVarslingStatusOppdatert**: Sender oppdatert status på bestilling.

## Dokumentasjon

Mer info om varsler finnes i [dokumentasjonen](https://navikt.github.io/tms-dokumentasjon/varsler/).

## Henvendelser

Spørsmål knyttet til koden eller prosjektet kan stilles som issues her på github.

## For NAV-ansatte

Interne henvendelser kan sendes via Slack i kanalen #min-side
