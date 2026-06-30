package pe.com.bootcamp.accountservice.mapper;

import org.springframework.stereotype.Component;
import pe.com.bootcamp.accountservice.dto.AccountResponse;
import pe.com.bootcamp.accountservice.dto.BankAccountBalanceResponse;
import pe.com.bootcamp.accountservice.dto.TransactionResponse;
import pe.com.bootcamp.accountservice.model.Account;
import pe.com.bootcamp.accountservice.model.Transaction;

@Component
public class AccountMapper {

    public AccountResponse toAccountResponse(Account account) {
        return AccountResponse.builder()
                .accountId(account.getAccountId())
                .accountNumber(account.getAccountNumber())
                .accountType(account.getAccountType())
                .balance(account.getBalance())
                .openingDate(account.getOpeningDate())
                .flagFreeCommisionMant(account.getFlagFreeCommisionMant())
                .maxMovMon(account.getMaxMovMon())
                .initialDate(account.getInitialDate())
                .status(account.getStatus())
                .build();
    }

    public BankAccountBalanceResponse toBankAccountBalanceResponse(
            Account account
    ) {
        return new BankAccountBalanceResponse(
                account.getAccountNumber(),
                account.getAccountType(),
                account.getBalance(),
                "PEN",
                "SOLES",
                account.getStatus()
        );
    }

    public TransactionResponse toTransactionResponse(Transaction transaction) {
        return new TransactionResponse(
                transaction.getTransactionId(),
                transaction.getAccountNumber(),
                transaction.getDocumentNumber(),
                transaction.getTransactionType(),
                transaction.getAmount(),
                transaction.getTransactionDate(),
                transaction.getStatus()
        );
    }

}
