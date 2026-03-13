package no.nav.tms.ekstern.varsling.bestilling

import kotlin.collections.map

object VarseltekstValidation {

    private val validators: List<VarselValidator> = listOf(
        SmstekstValidator,
        EposttittelValidator,
        EposttekstValidator,
        ForbyLinkIEksternVarslingValidator
    )

    fun validate(varsel: Varsel) {
        val errors = validators.map {
            it.validate(varsel)
        }.filterNot { it.isValid }

        if (errors.size > 1) {
            throw VarseltekstValidationException(
                message = "Fant ${errors.size} feil ved validering av varseltekster",
                explanation = errors.mapNotNull { it.explanation }
            )
        } else if (errors.size == 1) {
            throw VarseltekstValidationException(
                message = "Feil ved validering av varseltekster: ${errors.first().explanation}",
                explanation = errors.mapNotNull { it.explanation }
            )
        }
    }
}

class VarseltekstValidationException(message: String, val explanation: List<String> = emptyList()): IllegalArgumentException(message)

private interface VarselValidator {
    val description: String

    fun assertTrue(validatorFunction: () -> Boolean) = if (validatorFunction()) {
        ValidatorResult(true)
    } else {
        ValidatorResult(false, description)
    }

    fun validate(varsel: Varsel): ValidatorResult
}

private data class ValidatorResult(
    val isValid: Boolean,
    val explanation: String? = null
)

private object SmstekstValidator: VarselValidator {
    private const val MAX_LENGTH_SMS_VARSLINGSTEKST = 160
    override val description: String =
        "Sms-varsel kan ikke være tom string, og maks $MAX_LENGTH_SMS_VARSLINGSTEKST tegn"

    override fun validate(varsel: Varsel) = assertTrue {
        varsel.smsVarslingstekst
            ?.let {
                it.isNotBlank() && it.length <= MAX_LENGTH_SMS_VARSLINGSTEKST
            } ?: true
    }
}

private object EposttekstValidator: VarselValidator {
    private const val MAX_LENGTH_EPOST_VARSLINGSTEKST = 4000
    override val description: String =
        "Epost-tekst kan ikke være tom string, og maks $MAX_LENGTH_EPOST_VARSLINGSTEKST tegn"

    override fun validate(varsel: Varsel) = assertTrue {
        varsel.epostVarslingstekst
            ?.let {
                it.isNotBlank() && it.length <= MAX_LENGTH_EPOST_VARSLINGSTEKST
            } ?: true
    }
}

private object EposttittelValidator: VarselValidator {
    private const val MAX_LENGTH_EPOST_VARSLINGSTTITTEL = 40
    override val description: String =
        "Epost-tittel kan ikke være tom string, og maks $MAX_LENGTH_EPOST_VARSLINGSTTITTEL tegn"

    override fun validate(varsel: Varsel) = assertTrue {
        varsel.epostVarslingstittel
            ?.let {
                it.isNotBlank() && it.length <= MAX_LENGTH_EPOST_VARSLINGSTTITTEL
            } ?: true
    }
}

private object ForbyLinkIEksternVarslingValidator: VarselValidator {
    private const val URL_CHARACTERS = "[-a-zA-Z0-9@:%_\\+.~#?&//=]"
    private val linkLikePattern = "(https?://$URL_CHARACTERS+|$URL_CHARACTERS{2,256}\\.[a-z]{2,4}\\b(/$URL_CHARACTERS*)?)".toRegex()

    override val description = "Tekst i SMS/Epost kan ikke inneholde link, eller tekst som ser ut som link"

    override fun validate(varsel: Varsel) = assertTrue {
        smsHarIkkeLink(varsel) && epostHarIkkeLink(varsel)
    }

    private fun smsHarIkkeLink(varsel: Varsel): Boolean {
        return varsel.smsVarslingstekst == null || linkLikePattern.containsMatchIn(varsel.smsVarslingstekst).not()
    }

    private fun epostHarIkkeLink(varsel: Varsel): Boolean {
        return varsel.epostVarslingstekst == null || linkLikePattern.containsMatchIn(varsel.epostVarslingstekst).not()
    }
}
