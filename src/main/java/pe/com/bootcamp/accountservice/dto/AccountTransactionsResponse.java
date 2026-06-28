package pe.com.bootcamp.accountservice.dto;

import java.math.BigDecimal;
import java.util.List;

public record AccountTransactionsResponse(
        String customerId,
        String documentType,
        String documentNumber,
        String accountNumber,
        String accountType,
        BigDecimal currentBalance,
        List<TransactionResponse> transactions
) {
}
