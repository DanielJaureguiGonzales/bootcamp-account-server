package pe.com.bootcamp.accountservice.dto;

import java.math.BigDecimal;

public record BankAccountBalanceResponse(String accountNumber,
                                         String accountType,
                                         BigDecimal availableBalance,
                                         String currencyType,
                                         String currencyName,
                                         Boolean status) {
}
