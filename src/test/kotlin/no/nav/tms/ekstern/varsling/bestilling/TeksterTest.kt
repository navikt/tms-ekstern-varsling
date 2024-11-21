package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

class TeksterTest {

    @Test
    fun `setter default varseltekst for ett varsel uten overskrevet tekst`() {
        val oppgaveTekster = createVarsel(varseltype = Varseltype.Oppgave)
            .let { createEksternVarsling(it) }
            .let { bestemTekster(it) }

        oppgaveTekster.epostTekst shouldBe ForventetDefaultOppgaveTekst.epostTekst
        oppgaveTekster.epostTittel shouldBe ForventetDefaultOppgaveTekst.eposttittel
        oppgaveTekster.smsTekst shouldBe ForventetDefaultOppgaveTekst.smstekst


        val beskjedTekster = createVarsel(varseltype = Varseltype.Beskjed)
            .let { createEksternVarsling(it) }
            .let { bestemTekster(it) }

        beskjedTekster.epostTekst shouldBe ForventetDefaultBeskjedTekst.epostTekst
        beskjedTekster.epostTittel shouldBe ForventetDefaultBeskjedTekst.eposttittel
        beskjedTekster.smsTekst shouldBe ForventetDefaultBeskjedTekst.smstekst


        val innboksTekster = createVarsel(varseltype = Varseltype.Innboks)
            .let { createEksternVarsling(it) }
            .let { bestemTekster(it) }

        innboksTekster.epostTekst shouldBe ForventetDefaultInnboksTekst.epostTekst
        innboksTekster.epostTittel shouldBe ForventetDefaultInnboksTekst.eposttittel
        innboksTekster.smsTekst shouldBe ForventetDefaultInnboksTekst.smstekst
    }

    @Test
    fun `kan overskrive varseltekst`() {
        val smsVarslingstekst = "Overskrevet varseltekst for SMS"
        val epostVarslingstittel = "Overskrevet epost-tittel"
        val epostVarslingstekst = "<!DOCTYPE html><html><body>Overskrevet epost-tekst</body></html>\n"
        val varselTekst = createVarsel(
            Varseltype.Oppgave,
            smsVarslingstekst,
            epostVarslingstittel,
            epostVarslingstekst
        )
            .let { createEksternVarsling(it) }
            .let { bestemTekster(it) }

        varselTekst.epostTekst shouldBe epostVarslingstekst
        varselTekst.epostTittel shouldBe epostVarslingstittel
        varselTekst.smsTekst shouldBe smsVarslingstekst
    }

    @Test
    fun `wrapper eposttekst med markup dersom det mangler`() {
        val originalEpostTekst = "Overskrevet epost-tekst uten markup"
        val epostTekstMedMarkup =
            "<!DOCTYPE html><html><head><title>Varsel</title></head><body>$originalEpostTekst</body></html>\n"

        val varselTekst =
            createVarsel(Varseltype.Oppgave, epostVarslingstekst = originalEpostTekst)
                .let { createEksternVarsling(it) }
                .let { bestemTekster(it) }

        varselTekst.epostTekst shouldBe epostTekstMedMarkup
    }

    @Test
    fun `gir riktig tekst for batch-varsel`() {

        val oppgaver = createEksternVarsling(
            createVarsel(Varseltype.Oppgave),
            createVarsel(Varseltype.Oppgave),
            createVarsel(Varseltype.Oppgave)
        ).let { bestemTekster(it) }

        oppgaver.smsTekst shouldBe "Hei! Du har fått 3 oppgave(er) fra Nav. Logg inn på Nav for å se hva det gjelder. Vennlig hilsen Nav"
        oppgaver.epostTittel shouldBe "Du har fått varsler fra Nav"
        oppgaver.epostTekst shouldBe "<!DOCTYPE html><html><head><title>Varsel</title></head><body><p>Hei!</p><p>Du har fått 3 oppgave(er) fra Nav. Logg inn på Nav for å se hva det gjelder.</p><p>Vennlig hilsen</p><p>Nav</p></body></html>\n"

        val blandetVarsler = createEksternVarsling(
            createVarsel(Varseltype.Beskjed),
            createVarsel(Varseltype.Beskjed),
            createVarsel(Varseltype.Oppgave),
            createVarsel(Varseltype.Oppgave),
            createVarsel(Varseltype.Innboks),

            ).let { bestemTekster(it) }

        blandetVarsler.smsTekst shouldBe "Hei! Du har fått 3 beskjed(er) og 2 oppgave(er) fra Nav. Logg inn på Nav for å se hva det gjelder. Vennlig hilsen Nav"
        blandetVarsler.epostTittel shouldBe "Du har fått varsler fra Nav"
        blandetVarsler.epostTekst shouldBe "<!DOCTYPE html><html><head><title>Varsel</title></head><body><p>Hei!</p><p>Du har fått 3 beskjed(er) og 2 oppgave(er) fra Nav. Logg inn på Nav for å se hva det gjelder.</p><p>Vennlig hilsen</p><p>Nav</p></body></html>\n"
    }

    @Test
    fun `inaktive varsler påvirker ikke tekst`() {
        val oppgaver = createEksternVarsling(
            createVarsel(Varseltype.Oppgave, aktiv = true),
            createVarsel(Varseltype.Oppgave, aktiv = true),
            createVarsel(Varseltype.Oppgave, aktiv = true),
            createVarsel(Varseltype.Oppgave, aktiv = true),
            createVarsel(Varseltype.Oppgave, aktiv = false),
            createVarsel(Varseltype.Oppgave, aktiv = false)
        ).let { bestemTekster(it) }

        oppgaver.smsTekst shouldBe "Hei! Du har fått 4 oppgave(er) fra Nav. Logg inn på Nav for å se hva det gjelder. Vennlig hilsen Nav"
        oppgaver.epostTittel shouldBe "Du har fått varsler fra Nav"
        oppgaver.epostTekst shouldBe "<!DOCTYPE html><html><head><title>Varsel</title></head><body><p>Hei!</p><p>Du har fått 4 oppgave(er) fra Nav. Logg inn på Nav for å se hva det gjelder.</p><p>Vennlig hilsen</p><p>Nav</p></body></html>\n"

    }
}

private fun createVarsel(
    varseltype: Varseltype,
    smsVarslingstekst: String? = null,
    epostVarslingstittel: String? = null,
    epostVarslingstekst: String? = null,
    aktiv: Boolean = true,
    behandletAvLegacy: Boolean = false
) = Varsel(
    varselId = "1234",
    varseltype = varseltype,
    prefererteKanaler = listOf(),
    smsVarslingstekst = smsVarslingstekst,
    epostVarslingstittel = epostVarslingstittel,
    epostVarslingstekst = epostVarslingstekst,
    produsent = Produsent(cluster = "cluster", namespace = "namespace", appnavn = "appnavn"),
    aktiv = aktiv,
    behandletAvLegacy = behandletAvLegacy
)


private fun createEksternVarsling(
    vararg varsler: Varsel
) = EksternVarsling(
    sendingsId = "1234",
    ident = "12345",
    erBatch = false,
    erUtsattVarsel = false,
    varsler = varsler.asList(),
    utsending = null,
    ferdigstilt = null,
    opprettet = ZonedDateTimeHelper.nowAtUtc(),
    status = Sendingsstatus.Venter,
    eksternStatus = null,
    bestilling = null
)

private object ForventetDefaultOppgaveTekst {
    val eposttittel = "Du har fått en oppgave fra Nav"
    val smstekst =
        "Hei! Du har fått en ny oppgave fra Nav. Logg inn på Nav for å se hva oppgaven gjelder. Vennlig hilsen Nav"
    val epostTekst =
        "<!DOCTYPE html><html><head><title>Varsel</title></head><body><p>Hei!</p><p>Du har fått en ny oppgave fra Nav. Logg inn på Nav for å se hva oppgaven gjelder.</p><p>Vennlig hilsen</p><p>Nav</p></body></html>\n"
}

private object ForventetDefaultBeskjedTekst {
    val eposttittel = "Beskjed fra Nav"
    val smstekst =
        "Hei! Du har fått en ny beskjed fra Nav. Logg inn på Nav for å se hva beskjeden gjelder. Vennlig hilsen Nav"
    val epostTekst =
        "<!DOCTYPE html><html><head><title>Varsel</title></head><body><p>Hei!</p><p>Du har fått en ny beskjed fra Nav. Logg inn på Nav for å se hva beskjeden gjelder.</p><p>Vennlig hilsen</p><p>Nav</p></body></html>\n"
}

private object ForventetDefaultInnboksTekst {
    val eposttittel = "Du har fått en melding fra Nav"
    val smstekst =
        "Hei! Du har fått en ny melding fra Nav. Logg inn på Nav for å lese meldingen. Vennlig hilsen Nav"
    val epostTekst =
        "<!DOCTYPE html><html><head><title>Varsel</title></head><body><p>Hei!</p><p>Du har fått en ny melding fra Nav. Logg inn på Nav for å lese meldingen.</p><p>Vennlig hilsen</p><p>Nav</p></body></html>\n"
}
