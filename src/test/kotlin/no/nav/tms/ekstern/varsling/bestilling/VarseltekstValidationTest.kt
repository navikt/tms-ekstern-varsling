package no.nav.tms.ekstern.varsling.bestilling

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime
import java.util.UUID

class VarseltekstValidationTest {

    val validVarselOpprettet = varselOpprettet()

    private val text10Chars = "Laaaaaaang"

    @Test
    fun `godkjenner gyldig opprett-varsel event`() {
        shouldNotThrow<VarseltekstValidationException> {
            VarseltekstValidation.validate(validVarselOpprettet)
        }
    }

    @Test
    fun `feiler hvis eksterne varseltekster er for lange`() {
        shouldThrow<VarseltekstValidationException> {
            validVarselOpprettet.copy(
                smsVarslingstekst = text10Chars.repeat(17)
            ).let {
                VarseltekstValidation.validate(it)
            }
        }

        shouldThrow<VarseltekstValidationException> {
            validVarselOpprettet.copy(
                epostVarslingstittel = text10Chars.repeat(5)
            ).let {
                VarseltekstValidation.validate(it)
            }
        }

        shouldThrow<VarseltekstValidationException> {
            validVarselOpprettet.copy(
                epostVarslingstekst = text10Chars.repeat(401)
            ).let {
                VarseltekstValidation.validate(it)
            }
        }
    }

    @Test
    fun `feiler dersom innhold i sms eller epost inneholder lenke eller noe som ser ut som lenke`() {
        shouldThrow<VarseltekstValidationException> {
            validVarselOpprettet.copy(
                epostVarslingstekst = "Epost med lenke: https://lenke"
            ).let {
                VarseltekstValidation.validate(it)
            }
        }
        shouldThrow<VarseltekstValidationException> {
            validVarselOpprettet.copy(
                smsVarslingstekst = "Sms med lenke: http://www.nav.no"
            ).let {
                VarseltekstValidation.validate(it)
            }
        }
        shouldThrow<VarseltekstValidationException> {
            validVarselOpprettet.copy(
                smsVarslingstekst = "Tekst som ser ut.som lenke"
            ).let {
                VarseltekstValidation.validate(it)
            }
        }
        shouldNotThrow<VarseltekstValidationException> {
            validVarselOpprettet.copy(
                smsVarslingstekst = "Skrivefeil som ikke.heltser ut som lenke."
            ).let {
                VarseltekstValidation.validate(it)
            }
        }
    }

    private fun varselOpprettet() = Varsel(
        varseltype = Varseltype.Beskjed,
        varselId = UUID.randomUUID().toString(),
        prefererteKanaler = listOf(Kanal.SMS),
        smsVarslingstekst = "sms tekst",
        epostVarslingstittel = "epost tittel",
        epostVarslingstekst = "epost tekst",
        produsent = Produsent("cluster", "namespace", "appnavn"),
        aktiv = true,
        behandletAvLegacy = false
    )
}
