package pe.com.bootcamp.accountservice.factory;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import pe.com.bootcamp.accountservice.dto.AccountRequest;
import pe.com.bootcamp.accountservice.generator.AccountNumberGenerator;
import pe.com.bootcamp.accountservice.model.Account;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static pe.com.bootcamp.accountservice.constants.AccountConstants.ACCOUNT_TYPE_FIXED_TERM;
import static pe.com.bootcamp.accountservice.constants.AccountConstants.FIXED_TERM_DATE_FORMATTER;

@Component
@RequiredArgsConstructor
public class AccountFactory {

    private final AccountNumberGenerator accountNumberGenerator;

    public Account create(AccountRequest request) {

        return Account.builder()
                .accountNumber(accountNumberGenerator.generate())
                .accountType(normalize(request.accountType()))
                .balance(request.initialAmount())
                .status(true)
                .openingDate(LocalDateTime.now())
                .flagFreeCommisionMant(request.flagFreeCommisionMant())
                .maxMovMon(request.maxMovMon())
                .initialDate(resolveFixedTermInitialDate(request))
                .cantDays(request.cantDays())
                .build();
    }

    private LocalDateTime resolveFixedTermInitialDate(AccountRequest request) {

        if (!ACCOUNT_TYPE_FIXED_TERM.equals(normalize(request.accountType()))) {
            return null;
        }

        LocalDate date = LocalDate.parse(
                request.initialDate(),
                FIXED_TERM_DATE_FORMATTER
        );

        return date.atStartOfDay();
    }

    private String normalize(String value) {

        return value == null
                ? ""
                : value.trim().toUpperCase();
    }

}
