package pe.com.bootcamp.accountservice.service;

import pe.com.bootcamp.accountservice.dto.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AccountService {

    Mono<AccountResponse> createAccountCustomer(AccountRequest accountRequest);

    Mono<OperationCompleted> transactions(OperationRequest operationRequest, String idOperation);

    Flux<AccountResponse> getAccountByDocumentNumber(String documentNumber, String documentType);
    Mono<AccountBalancesResponse> getAccountBalances(BalanceRequest request);
    Mono<AccountTransactionsResponse> getAccountTransactions(
            AccountTransactionsRequest request
    );
}
