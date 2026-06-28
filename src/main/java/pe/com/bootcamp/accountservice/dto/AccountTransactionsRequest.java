package pe.com.bootcamp.accountservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record AccountTransactionsRequest(
                                         String documentType,
                                         String documentNumber,
                                         @NotBlank(message = "Account number is required")
                                         String accountNumber) {
}
