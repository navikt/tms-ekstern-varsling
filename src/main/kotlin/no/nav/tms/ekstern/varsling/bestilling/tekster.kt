package no.nav.tms.ekstern.varsling.bestilling

data class Tekster(
    val smsTekst: String,
    val epostTittel: String,
    val epostTekst: String
)

fun bestemTekster(eksternVarsling: EksternVarsling): Tekster {
    if (eksternVarsling.varsler.size > 1) {
        return batchTekster(eksternVarsling)
    }

    val varsel = eksternVarsling.varsler.first()

    val standardtekster = when(varsel.varseltype) {
        Varseltype.Oppgave -> OppgaveTekster
        Varseltype.Beskjed -> BeskjedTekster
        Varseltype.Innboks -> InnboksTekster
    }

    return Tekster(
        smsTekst = varsel.smsVarslingstekst ?: standardtekster.smstekst,
        epostTittel = varsel.epostVarslingstittel ?: standardtekster.eposttittel,
        epostTekst = varsel.epostVarslingstekst
            ?.let { addMarkupIfMissing(it) }
            ?: standardtekster.epostTekst,
    )
}

private fun addMarkupIfMissing(epostTekst: String): String {
    val markupPattern = "<!DOCTYPE html>".toRegex()

    return if (markupPattern.containsMatchIn(epostTekst)) {
        epostTekst
    } else {
        EpostMal.leggTilMarkup(epostTekst)
    }
}

private fun batchTekster(eksternVarsling: EksternVarsling): Tekster {
    val antallTekst = eksternVarsling.varsler
        .groupBy { it.varseltype.alias }
        .mapValues { it.value.size }
        .map { (type, antall) -> "$antall $type(er)" }
        .joinToString(" og ")

    return Tekster(
        smsTekst =
            "Hei! Du har fått $antallTekst fra NAV. Logg inn på NAV for å se hva det gjelder. Vennlig hilsen NAV",
        epostTittel = "Du har fått varsler fra NAV",
        epostTekst = EpostBatchTekst.tekst(antallTekst)
    )
}

private object EpostBatchTekst {
    private val template = this::class.java.getResource("/texts/epost_batch_template.txt")!!.readText(Charsets.UTF_8)

    fun tekst(antallTekst: String) = template.replace("{{VARSELTEKST}}", antallTekst)
}

private object EpostMal {
    private val template = this::class.java.getResource("/texts/epost_mal.txt")!!.readText(Charsets.UTF_8)

    fun leggTilMarkup(innhold: String) = template.replace("{{VARSELTEKST}}", innhold)
}

private interface Standardtekster {
    val eposttittel: String
    val smstekst: String
    val epostTekstfil: String
    val epostTekst: String
}

private object OppgaveTekster: Standardtekster {
    override val eposttittel = "Du har fått en oppgave fra NAV"
    override val smstekst =
        "Hei! Du har fått en ny oppgave fra NAV. Logg inn på NAV for å se hva oppgaven gjelder. Vennlig hilsen NAV"
    override val epostTekstfil = "epost_oppgave.txt"
    override val epostTekst = this::class.java.getResource("/texts/$epostTekstfil")!!.readText(Charsets.UTF_8)
}

private object BeskjedTekster: Standardtekster {
    override val eposttittel = "Beskjed fra NAV"
    override val smstekst = "Hei! Du har fått en ny beskjed fra NAV. Logg inn på NAV for å se hva beskjeden gjelder. Vennlig hilsen NAV"
    override val epostTekstfil = "epost_beskjed.txt"
    override val epostTekst = this::class.java.getResource("/texts/${epostTekstfil}")!!.readText(Charsets.UTF_8)
}

private object InnboksTekster: Standardtekster {
    override val eposttittel = "Du har fått en melding fra NAV"
    override val smstekst = "Hei! Du har fått en ny melding fra NAV. Logg inn på NAV for å lese meldingen. Vennlig hilsen NAV"
    override val epostTekstfil = "epost_innboks.txt"
    override val epostTekst = this::class.java.getResource("/texts/${epostTekstfil}")!!.readText(Charsets.UTF_8)
}
