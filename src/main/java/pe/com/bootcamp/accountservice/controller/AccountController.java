package pe.com.bootcamp.accountservice.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pe.com.bootcamp.accountservice.dto.*;
import pe.com.bootcamp.accountservice.service.AccountService;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @PostMapping
    private Mono<ResponseEntity<AccountResponse>> createAccount(@Valid @RequestBody AccountRequest accountRequest){

        return accountService.createAccountCustomer(accountRequest)
                .map( accountResponse ->  ResponseEntity.status(HttpStatus.CREATED)
                        .body(accountResponse));


    }

    @PostMapping("/operations")
    private Mono<ResponseEntity<OperationCompleted>> realizeOperations(@RequestParam("id") String operation,
                                                                       @RequestBody @Valid OperationRequest operationRequest){
        return accountService.transactions(operationRequest, operation)
                .map(operationCompleted -> ResponseEntity.status(HttpStatus.CREATED)
                        .body(operationCompleted));
    }

    @GetMapping("/document")
    private Mono<ResponseEntity<List<AccountResponse>>> getAccountByDocumentNumber(@RequestParam("document-number") String documentNumber,@RequestParam("document-type") String documentType){
        return accountService.getAccountByDocumentNumber(documentNumber,documentType).collectList().map( accountResponse ->  ResponseEntity.status(HttpStatus.OK)
                .body(accountResponse));
    }

    @DeleteMapping
    public Mono<ResponseEntity<Void>> deleteAccount(
            @RequestBody @Valid AccountDeleteRequest request
    ) {
        return accountService.deleteAccount(request)
                .then(Mono.fromSupplier(() -> ResponseEntity.noContent().build()));
    }

    @GetMapping("/balances")
    public Mono<ResponseEntity<AccountBalancesResponse>> getAccountBalances(
            @RequestBody BalanceRequest request
    ) {
        return accountService.getAccountBalances(request)
                .map(ResponseEntity::ok);
    }

    @GetMapping("/transactions")
    public Mono<ResponseEntity<AccountTransactionsResponse>> getAccountTransactions(
            @RequestBody @Valid AccountTransactionsRequest request
    ) {
        return accountService.getAccountTransactions(request)
                .map(ResponseEntity::ok);
    }
}
