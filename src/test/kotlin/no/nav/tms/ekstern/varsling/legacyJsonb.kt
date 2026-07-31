package no.nav.tms.ekstern.varsling

import kotliquery.queryOf
import no.nav.tms.common.postgres.JsonbHelper.toJsonb
import no.nav.tms.common.postgres.PostgresDatabase

fun PostgresDatabase.insertEksternVarslingWithLegacyVarsel(eksternVarsling: EksternVarsling) {
    update {
        queryOf(
            """
                insert into ekstern_varsling(sendingsId, ident, erBatch, erUtsattVarsel, varsler, utsending, ferdigstilt, opprettet, status, bestilling)
                values (:sendingsId, :ident, :erBatch, :erUtsattVarsel, :varsler, :utsending, :ferdigstilt, :opprettet, :status, :bestilling)
            """,
            mapOf(
                "sendingsId" to eksternVarsling.sendingsId,
                "ident" to eksternVarsling.ident,
                "erBatch" to eksternVarsling.erBatch,
                "erUtsattVarsel" to eksternVarsling.erUtsattVarsel,
                "varsler" to eksternVarsling.varsler.filter{ it.legacyJsonb }.toJsonb(),
                "utsending" to eksternVarsling.utsending,
                "ferdigstilt" to eksternVarsling.ferdigstilt,
                "status" to eksternVarsling.status.name,
                "bestilling" to eksternVarsling.bestilling?.toJsonb(),
                "opprettet" to eksternVarsling.opprettet,
            )
        )
    }
    eksternVarsling.varsler.filterNot { it.legacyJsonb }.forEach { varsel ->
        update {
            queryOf(
                """
                insert into varsel(varselId, sendingsId, varseltype, preferertKanal, smsVarslingstekst, epostVarslingstittel, epostVarslingstekst, aktiv, produsent, opprettet, inaktivert)
                values(:varselId, :sendingsId, :varseltype, :preferertKanal, :smsVarslingstekst, :epostVarslingstittel, :epostVarslingstekst, :aktiv, :produsent, :opprettet, :inaktivert)
            """,
                mapOf(
                    "sendingsId" to eksternVarsling.sendingsId,
                    "varselId" to varsel.varselId,
                    "varseltype" to varsel.varseltype.name,
                    "preferertKanal" to varsel.preferertKanal?.name,
                    "smsVarslingstekst" to varsel.smsVarslingstekst,
                    "epostVarslingstittel" to varsel.epostVarslingstittel,
                    "epostVarslingstekst" to varsel.epostVarslingstekst,
                    "aktiv" to varsel.aktiv,
                    "produsent" to varsel.produsent.toJsonb(),
                    "opprettet" to varsel.opprettet,
                    "inaktivert" to varsel.inaktivert
                )
            )
        }
    }
}
