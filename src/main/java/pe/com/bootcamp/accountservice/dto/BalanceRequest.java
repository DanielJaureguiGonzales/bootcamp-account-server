package pe.com.bootcamp.accountservice.dto;

public record BalanceRequest(
        String documentType,
        String documentNumber
) {
}
