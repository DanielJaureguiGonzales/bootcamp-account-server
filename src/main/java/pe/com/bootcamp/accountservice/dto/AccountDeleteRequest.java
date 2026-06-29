package pe.com.bootcamp.accountservice.dto;

import jakarta.validation.constraints.NotBlank;

public record AccountDeleteRequest(
        String documentType,
        String documentNumber,

        @NotBlank(message = "Account number is required")
        String accountNumber
) {
}
