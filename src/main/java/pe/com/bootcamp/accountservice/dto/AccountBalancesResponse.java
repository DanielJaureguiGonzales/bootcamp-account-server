package pe.com.bootcamp.accountservice.dto;

import java.util.List;

public record AccountBalancesResponse(String customerId,
                                      String documentType,
                                      String documentNumber,
                                      List<BankAccountBalanceResponse> accounts) {
}
