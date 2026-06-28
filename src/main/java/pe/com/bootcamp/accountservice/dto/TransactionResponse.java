package pe.com.bootcamp.accountservice.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record TransactionResponse(
        String transactionId,
        String accountNumber,
        String documentNumber,
        String transactionType,
        BigDecimal amount,
        LocalDateTime transactionDate,
        Boolean status
) {
}
