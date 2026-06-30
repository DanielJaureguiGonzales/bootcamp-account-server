package pe.com.bootcamp.accountservice.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import pe.com.bootcamp.accountservice.dto.AccountBalancesResponse;
import pe.com.bootcamp.accountservice.dto.AccountDeleteRequest;
import pe.com.bootcamp.accountservice.dto.AccountParticipantRequest;
import pe.com.bootcamp.accountservice.dto.AccountRequest;
import pe.com.bootcamp.accountservice.dto.AccountResponse;
import pe.com.bootcamp.accountservice.dto.AccountTransactionsRequest;
import pe.com.bootcamp.accountservice.dto.AccountTransactionsResponse;
import pe.com.bootcamp.accountservice.dto.AccountValidationResult;
import pe.com.bootcamp.accountservice.dto.BalanceRequest;
import pe.com.bootcamp.accountservice.dto.CustomerResponse;
import pe.com.bootcamp.accountservice.dto.CustomerSummaryResponse;
import pe.com.bootcamp.accountservice.dto.DocumentNumbersRequest;
import pe.com.bootcamp.accountservice.dto.OperationCompleted;
import pe.com.bootcamp.accountservice.dto.OperationRequest;
import pe.com.bootcamp.accountservice.exceptions.BusinessValidationException;
import pe.com.bootcamp.accountservice.exceptions.ResourceNotFoundException;
import pe.com.bootcamp.accountservice.factory.AccountFactory;
import pe.com.bootcamp.accountservice.factory.AccountParticipantFactory;
import pe.com.bootcamp.accountservice.factory.TransactionFactory;
import pe.com.bootcamp.accountservice.mapper.AccountMapper;
import pe.com.bootcamp.accountservice.model.Account;
import pe.com.bootcamp.accountservice.model.AccountParticipant;
import pe.com.bootcamp.accountservice.model.Transaction;
import pe.com.bootcamp.accountservice.repository.AccountParticipantRepository;
import pe.com.bootcamp.accountservice.repository.AccountRepository;
import pe.com.bootcamp.accountservice.repository.TransactionRepository;
import pe.com.bootcamp.accountservice.service.AccountService;
import pe.com.bootcamp.accountservice.service.rest.CustomerResponseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static pe.com.bootcamp.accountservice.constants.AccountConstants.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountServiceImpl implements AccountService {

    private final AccountRepository accountRepository;
    private final AccountParticipantRepository accountParticipantRepository;
    private final TransactionRepository transactionsRepository;
    private final CustomerResponseClient client;

    private final AccountFactory accountFactory;
    private final TransactionFactory transactionFactory;
    private final AccountParticipantFactory accountParticipantFactory;

    private final AccountMapper accountMapper;

    @Override
    public Mono<AccountResponse> createAccountCustomer(AccountRequest accountRequest) {

        log.info(
                "Starting account creation. documentType={}, documentNumber={}, accountType={}",
                accountRequest.documentType(),
                maskValue(accountRequest.documentNumber()),
                accountRequest.accountType()
        );

        return validateAccountRequest(accountRequest)
                .then(client.getCustomerResponse(accountRequest))
                .doOnNext(customerResponse ->
                        log.info(
                                "Customer retrieved for account creation. customerId={}, documentType={}",
                                customerResponse.id(),
                                customerResponse.documentType()
                        )
                )
                .flatMap(customerResponse ->
                        validateAccountByCustomerType(customerResponse, accountRequest)
                                .flatMap(validationResult ->
                                        createAccountAndParticipant(
                                                customerResponse,
                                                accountRequest,
                                                validationResult
                                        )
                                )
                )
                .map(accountMapper::toAccountResponse)
                .doOnSuccess(response ->
                        log.info(
                                "Account created successfully. accountId={}, accountNumber={}, accountType={}",
                                response.accountId(),
                                maskValue(response.accountNumber()),
                                response.accountType()
                        )
                )
                .doOnError(BusinessValidationException.class, error ->
                        log.warn(
                                "Account creation rejected by validation. documentType={}, documentNumber={}, accountType={}",
                                accountRequest.documentType(),
                                maskValue(accountRequest.documentNumber()),
                                accountRequest.accountType()
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn(
                                "Account creation rejected because customer was not found. documentType={}, documentNumber={}",
                                accountRequest.documentType(),
                                maskValue(accountRequest.documentNumber())
                        )
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error creating account",
                                error
                        )
                );
    }

    @Override
    public Mono<OperationCompleted> transactions(
            OperationRequest operationRequest,
            String idOperation
    ) {
        String operation = normalize(idOperation);

        log.info(
                "Starting account transaction. operation={}, documentType={}, documentNumber={}, accountNumber={}",
                operation,
                operationRequest.documentType(),
                maskValue(operationRequest.documentNumber()),
                maskValue(operationRequest.accountNumber())
        );


        return validateOperationRequest(operationRequest, idOperation)
                .then(getAccountAccessContext(
                        operationRequest.documentNumber(),
                        operationRequest.documentType(),
                        operationRequest.accountNumber(),
                        List.of(ROLE_HOLDER, ROLE_AUTHORIZED_SIGNER),
                        "Customer is not allowed to operate this account"
                )).flatMap(context ->
                        applyTransactionOperation(
                                operationRequest,
                                context.account(),
                                operation,
                                context.customer().id()
                        )
                )
                .doOnSuccess(response ->
                        log.info(
                                "Account transaction completed successfully. operation={}, accountId={}, customerId={}",
                                response.operation(),
                                response.accountId(),
                                response.customerId()
                        )
                )
                .doOnError(BusinessValidationException.class, error ->
                        log.warn(
                                "Account transaction rejected by validation. operation={}, documentType={}, documentNumber={}, accountNumber={}",
                                operation,
                                operationRequest.documentType(),
                                maskValue(operationRequest.documentNumber()),
                                maskValue(operationRequest.accountNumber())
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn(
                                "Account transaction rejected because resource was not found. operation={}, accountNumber={}",
                                operation,
                                maskValue(operationRequest.accountNumber())
                        )
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error processing account transaction",
                                error
                        )
                );
    }


    private Mono<Account> findActiveAccountByNumber(String accountNumber) {

        return accountRepository.findByAccountNumberAndStatus(accountNumber, true)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException(
                        "Account",
                        "accountNumber",
                        accountNumber
                )));
    }

    private Mono<CustomerResponse> findCustomerByDocument(
            String documentNumber,
            String documentType
    ) {

        return client.getCustomerResponseByCustomer(documentNumber, documentType)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException(
                        "Customer",
                        "documentNumber",
                        documentNumber
                )));
    }

    private Mono<OperationCompleted> applyTransactionOperation(
            OperationRequest operationRequest,
            Account account,
            String operation,
            String customerId
    ) {

        BigDecimal currentBalance = account.getBalance();

        return calculateNewBalance(
                currentBalance,
                operationRequest.amount(),
                operation
        )
                .flatMap(newBalance -> {
                    account.setBalance(newBalance);

                    return saveTransaction(
                            operationRequest,
                            account,
                            operation,
                            customerId
                    );
                });
    }

    private Mono<BigDecimal> calculateNewBalance(
            BigDecimal currentBalance,
            BigDecimal amount,
            String operation
    ) {

        return switch (operation) {
            case OPERATION_DEPOSIT -> Mono.just(
                    currentBalance.add(amount)
            );

            case OPERATION_WITHDRAW -> calculateWithdrawBalance(
                    currentBalance,
                    amount
            );

            default -> Mono.error(new RuntimeException(
                    "Invalid operation: " + operation
            ));
        };
    }

    private Mono<BigDecimal> calculateWithdrawBalance(
            BigDecimal currentBalance,
            BigDecimal amount
    ) {

        if (amount.compareTo(currentBalance) > 0) {
            return Mono.error(new RuntimeException(
                    "Insufficient balance"
            ));
        }

        return Mono.just(currentBalance.subtract(amount));
    }

    private Mono<Void> validateDocumentRequest(
            String documentType,
            String documentNumber
    ) {

        Map<String, String> errors = new HashMap<>();

        validateDocument(
                errors,
                "documentType",
                "documentNumber",
                documentType,
                documentNumber
        );

        if (!errors.isEmpty()) {
            return Mono.error(new BusinessValidationException(errors));
        }

        return Mono.empty();
    }

    private Mono<Void> validateCustomerHasRoleInAccount(
            Account account,
            String customerId,
            List<String> allowedRoles,
            String errorMessage
    ) {

        return accountParticipantRepository
                .existsByAccountIdAndCustomerIdAndParticipantRoleInAndStatus(
                        account.getAccountId(),
                        customerId,
                        allowedRoles,
                        true
                )
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new RuntimeException(errorMessage)))
                .then();
    }

    private Mono<AccountAccessContext> getAccountAccessContext(
            String documentNumber,
            String documentType,
            String accountNumber,
            List<String> allowedRoles,
            String errorMessage
    ) {

        return findCustomerByDocument(documentNumber, documentType)
                .flatMap(customerResponse ->
                        findActiveAccountByNumber(accountNumber)
                                .flatMap(account ->
                                        validateCustomerHasRoleInAccount(
                                                account,
                                                customerResponse.id(),
                                                allowedRoles,
                                                errorMessage
                                        ).thenReturn(new AccountAccessContext(
                                                customerResponse,
                                                account
                                        ))
                                )
                );
    }

    @Override
    public Flux<AccountResponse> getAccountByDocumentNumber(
            String documentNumber,
            String documentType
    ) {

        log.info(
                "Starting accounts search by customer document. documentType={}, documentNumber={}",
                documentType,
                maskValue(documentNumber)
        );

        return validateDocumentRequest(documentType, documentNumber)
                .thenMany(findCustomerByDocument(documentNumber, documentType)
                        .flatMapMany(customerResponse ->
                                findActiveAccountsByCustomerId(customerResponse.id())
                        )
                .map(accountMapper::toAccountResponse))
                .doOnComplete(() ->
                        log.info(
                                "Accounts search by customer document completed. documentType={}, documentNumber={}",
                                documentType,
                                maskValue(documentNumber)
                        )
                )
                .doOnError(BusinessValidationException.class, error ->
                        log.warn(
                                "Accounts search rejected by document validation. documentType={}, documentNumber={}",
                                documentType,
                                maskValue(documentNumber)
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn(
                                "Accounts search rejected because customer was not found. documentType={}, documentNumber={}",
                                documentType,
                                maskValue(documentNumber)
                        )
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error searching accounts by customer document",
                                error
                        )
                );

    }

    @Override
    public Mono<AccountBalancesResponse> getAccountBalances(BalanceRequest request) {

        log.info(
                "Starting account balances search. documentType={}, documentNumber={}",
                request.documentType(),
                maskValue(request.documentNumber())
        );

        return validateDocumentRequest(
                    request.documentType(),
                    request.documentNumber()
                )
                .then(findCustomerByDocument(
                        request.documentNumber(),
                        request.documentType()
                ))
                .flatMap(customerResponse ->
                        findActiveAccountsByCustomerId(customerResponse.id())
                                .map(accountMapper::toBankAccountBalanceResponse)
                                .collectList()
                                .map(accounts -> new AccountBalancesResponse(
                                        customerResponse.id(),
                                        request.documentType(),
                                        request.documentNumber(),
                                        accounts
                                ))
                )
                .doOnSuccess(response ->
                        log.info(
                                "Account balances search completed. customerId={}, accountCount={}",
                                response.customerId(),
                                response.accounts().size()
                        )
                )
                .doOnError(BusinessValidationException.class, error ->
                        log.warn(
                                "Account balances search rejected by validation. documentType={}, documentNumber={}",
                                request.documentType(),
                                maskValue(request.documentNumber())
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn(
                                "Account balances search rejected because customer was not found. documentType={}, documentNumber={}",
                                request.documentType(),
                                maskValue(request.documentNumber())
                        )
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error searching account balances",
                                error
                        )
                );
    }

    @Override
    public Mono<AccountTransactionsResponse> getAccountTransactions(
            AccountTransactionsRequest request
    ) {

        log.info(
                "Starting account transactions search. documentType={}, documentNumber={}, accountNumber={}",
                request.documentType(),
                maskValue(request.documentNumber()),
                maskValue(request.accountNumber())
        );

        return validateDocumentRequest(
                    request.documentType(),
                    request.documentNumber()
                )
                .then(getAccountAccessContext(
                        request.documentNumber(),
                        request.documentType(),
                        request.accountNumber(),
                        List.of(ROLE_HOLDER, ROLE_AUTHORIZED_SIGNER),
                        "Customer is not allowed to view transactions of this account"
                ))
                .flatMap(context ->
                        transactionsRepository
                                .findByAccountIdAndStatusOrderByTransactionDateDesc(
                                        context.account().getAccountId(),
                                        true
                                )
                                .map(accountMapper::toTransactionResponse)
                                .collectList()
                                .map(transactions -> new AccountTransactionsResponse(
                                        context.customer().id(),
                                        request.documentType(),
                                        request.documentNumber(),
                                        context.account().getAccountNumber(),
                                        context.account().getAccountType(),
                                        context.account().getBalance(),
                                        transactions
                                ))
                )
                .doOnSuccess(response ->
                        log.info(
                                "Account transactions search completed. customerId={}, accountNumber={}, transactionCount={}",
                                response.customerId(),
                                maskValue(response.accountNumber()),
                                response.transactions().size()
                        )
                )
                .doOnError(BusinessValidationException.class, error ->
                        log.warn(
                                "Account transactions search rejected by validation. documentType={}, documentNumber={}, accountNumber={}",
                                request.documentType(),
                                maskValue(request.documentNumber()),
                                maskValue(request.accountNumber())
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn(
                                "Account transactions search rejected because resource was not found. accountNumber={}",
                                maskValue(request.accountNumber())
                        )
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error searching account transactions",
                                error
                        )
                );
    }

    @Override
    public Mono<Void> deleteAccount(AccountDeleteRequest request) {

        log.info(
                "Starting logical account deletion. documentType={}, documentNumber={}, accountNumber={}",
                request.documentType(),
                maskValue(request.documentNumber()),
                maskValue(request.accountNumber())
        );


        return validateDocumentRequest(
                    request.documentType(),
                    request.documentNumber()
                )
                .then(getAccountAccessContext(
                        request.documentNumber(),
                        request.documentType(),
                        request.accountNumber(),
                        List.of(ROLE_HOLDER),
                        "Only account holder can delete this account"
                ))
                .map(AccountAccessContext::account)
                .flatMap(this::deleteAccountAndParticipants)
                .doOnSuccess(unused ->
                        log.info(
                                "Account logically deleted successfully. accountNumber={}",
                                maskValue(request.accountNumber())
                        )
                )
                .doOnError(BusinessValidationException.class, error ->
                        log.warn(
                                "Account deletion rejected by validation. documentType={}, documentNumber={}, accountNumber={}",
                                request.documentType(),
                                maskValue(request.documentNumber()),
                                maskValue(request.accountNumber())
                        )
                )
                .doOnError(ResourceNotFoundException.class, error ->
                        log.warn(
                                "Account deletion rejected because resource was not found. accountNumber={}",
                                maskValue(request.accountNumber())
                        )
                )
                .doOnError(error ->
                        logUnexpectedError(
                                "Unexpected error deleting account",
                                error
                        )
                );

    }

    private Mono<Account> createAccountAndParticipant(
            CustomerResponse customer,
            AccountRequest accountRequest,
            AccountValidationResult validationResult
    ) {

        Account account = accountFactory.create(accountRequest);

        return accountRepository.save(account)
                .flatMap(savedAccount ->
                        createParticipants(
                                savedAccount,
                                customer,
                                accountRequest,
                                validationResult
                        ).thenReturn(savedAccount)
                );
    }

    private Mono<Void> deleteAccountAndParticipants(Account account) {


        log.info(
                "Disabling account and active participants. accountId={}, accountNumber={}",
                account.getAccountId(),
                maskValue(account.getAccountNumber())
        );


        account.setStatus(false);

        return accountRepository.save(account)
                .thenMany(accountParticipantRepository.findByAccountIdAndStatus(
                        account.getAccountId(),
                        true
                ))
                .flatMap(participant -> {
                    participant.setStatus(false);
                    return accountParticipantRepository.save(participant);
                })
                .then()
                .doOnSuccess(unused ->
                        log.info(
                                "Account and active participants disabled successfully. accountId={}, accountNumber={}",
                                account.getAccountId(),
                                maskValue(account.getAccountNumber())
                        )
                );
    }

    private Mono<OperationCompleted> saveTransaction(
            OperationRequest operationRequest,
            Account account,
            String operation,
            String customerId
    ) {

        log.info(
                "Saving account transaction. operation={}, accountId={}, accountNumber={}, customerId={}",
                operation,
                account.getAccountId(),
                maskValue(account.getAccountNumber()),
                customerId
        );

        return accountRepository.save(account)
                .flatMap(accountSaved -> {
                    Transaction transaction = transactionFactory.create(
                            operationRequest,
                            accountSaved,
                            operation,
                            customerId
                    );

                    return transactionsRepository.save(transaction);
                })
                .doOnSuccess(transaction ->
                        log.info(
                                "Transaction saved successfully. transactionId={}, operation={}, accountId={}, customerId={}",
                                transaction.getTransactionId(),
                                operation,
                                transaction.getAccountId(),
                                transaction.getCustomerId()
                        )
                )
                .map(transaction ->  transactionFactory.toOperationCompleted(
                                transaction,
                                operation
                        )
                );
    }


    private Mono<AccountValidationResult> validateAccountByCustomerType(
            CustomerResponse customerResponse,
            AccountRequest accountRequest
    ) {

        String customerType = normalize(customerResponse.documentType());

        return switch (customerType) {
            case DOCUMENT_TYPE_BUSINESS -> validateAccountBusinessCustomer(accountRequest)
                    .map(AccountValidationResult::new);

            case DOCUMENT_TYPE_PERSONAL -> validateAccountMaxPerCustomer(
                    customerResponse,
                    accountRequest
            ).thenReturn(AccountValidationResult.empty());

            default -> Mono.error(new RuntimeException(
                    "Invalid customer type: " + customerType
            ));
        };
    }

    private Mono<List<CustomerSummaryResponse>> validateAccountBusinessCustomer(
            AccountRequest request
    ) {

        String accountType = normalize(request.accountType());

        if (!ACCOUNT_TYPE_CHECKING.equals(accountType)) {
            return Mono.error(new RuntimeException(
                    "Business customers can only create checking accounts"
            ));
        }

        List<AccountParticipantRequest> participants = getSafeParticipants(request);

        if (participants.isEmpty()) {
            return Mono.just(List.of());
        }

        List<String> documentNumbers = participants.stream()
                .map(AccountParticipantRequest::documentNumber)
                .map(this::normalizeText)
                .distinct()
                .toList();

        return client.getCustomerSummaryList(
                        DocumentNumbersRequest.builder()
                                .documentNumbers(documentNumbers)
                                .build()
                )
                .flatMap(customers -> {

                    if (customers.size() != documentNumbers.size()) {
                        return Mono.error(new RuntimeException(
                                "Deben existir todos los usuarios en el sistema"
                        ));
                    }

                    return Mono.just(customers);
                });
    }

    private Mono<Void> validateAccountMaxPerCustomer(
            CustomerResponse customer,
            AccountRequest request
    ) {

        String accountType = normalize(request.accountType());

        if (ACCOUNT_TYPE_FIXED_TERM.equals(accountType)) {
            return Mono.empty();
        }

        return accountParticipantRepository
                .findByCustomerIdAndParticipantRoleAndStatus(
                        customer.id(),
                        ROLE_HOLDER,
                        true
                )
                .map(AccountParticipant::getAccountId)
                .collectList()
                .flatMap(accountIds -> {
                    if (accountIds.isEmpty()) {
                        return Mono.just(0L);
                    }

                    return accountRepository.countByAccountIdInAndAccountTypeAndStatus(
                            accountIds,
                            accountType,
                            true
                    );
                })
                .flatMap(count -> {
                    if (count > 0) {
                        return Mono.error(new RuntimeException(
                                "No se permite tener más cuentas de tipo: "
                                        + getAccountTypeName(accountType)
                        ));
                    }

                    return Mono.empty();
                });
    }


    private Mono<Void> createParticipants(
            Account savedAccount,
            CustomerResponse mainCustomer,
            AccountRequest accountRequest,
            AccountValidationResult validationResult
    ) {

        String customerType = normalize(mainCustomer.documentType());

        return switch (customerType) {
            case DOCUMENT_TYPE_PERSONAL -> createPersonalParticipants(
                    savedAccount,
                    mainCustomer
            );

            case DOCUMENT_TYPE_BUSINESS -> createBusinessParticipants(
                    savedAccount,
                    mainCustomer,
                    accountRequest,
                    validationResult
            );

            default -> Mono.error(new RuntimeException("Invalid customer type"));
        };
    }

    private Mono<Void> createPersonalParticipants(
            Account savedAccount,
            CustomerResponse mainCustomer
    ) {

        AccountParticipant participant = accountParticipantFactory.create(
                savedAccount.getAccountId(),
                mainCustomer.id(),
                ROLE_HOLDER
        );

        return accountParticipantRepository.save(participant).then();
    }

    private Mono<Void> createBusinessParticipants(
            Account savedAccount,
            CustomerResponse mainCustomer,
            AccountRequest accountRequest,
            AccountValidationResult validationResult
    ) {

        List<AccountParticipant> participantsToSave = new ArrayList<>();

        AccountParticipant mainHolder = accountParticipantFactory.create(
                savedAccount.getAccountId(),
                mainCustomer.id(),
                ROLE_HOLDER
        );


        participantsToSave.add(mainHolder);

        List<AccountParticipantRequest> requestParticipants =
                getSafeParticipants(accountRequest);

        Map<String, CustomerSummaryResponse> customerMap =
                validationResult.participantCustomers()
                        .stream()
                        .collect(Collectors.toMap(
                                customer -> normalizeText(customer.documentNumber()),
                                Function.identity(),
                                (current, duplicated) -> current
                        ));

        for (AccountParticipantRequest participantRequest : requestParticipants) {

            String participantDocumentNumber =
                    normalizeText(participantRequest.documentNumber());

            CustomerSummaryResponse participantCustomer =
                    customerMap.get(participantDocumentNumber);

            if (participantCustomer == null) {
                return Mono.error(new RuntimeException(
                        "Participant customer not found with documentNumber: "
                                + participantDocumentNumber
                ));
            }

            AccountParticipant participant = accountParticipantFactory.create(
                    savedAccount.getAccountId(),
                    participantCustomer.id(),
                    participantRequest.participantRole()
            );

            participantsToSave.add(participant);
        }

        return accountParticipantRepository.saveAll(participantsToSave).then();
    }

    private Flux<Account> findActiveAccountsByCustomerId(String customerId) {

        return accountParticipantRepository
                .findByCustomerIdAndStatus(customerId, true)
                .map(AccountParticipant::getAccountId)
                .distinct()
                .collectList()
                .flatMapMany(accountIds -> {
                    if (accountIds.isEmpty()) {
                        return Flux.empty();
                    }

                    return accountRepository.findByAccountIdInAndStatus(
                            accountIds,
                            true
                    );
                });
    }

    private Mono<Void> validateAccountRequest(AccountRequest request) {

        Map<String, String> errors = new HashMap<>();

        validateDocument(
                errors,
                "documentType",
                "documentNumber",
                request.documentType(),
                request.documentNumber()
        );

        validateParticipants(errors, request);

        if (!errors.isEmpty()) {
            return Mono.error(new BusinessValidationException(errors));
        }

        return Mono.empty();
    }

    private Mono<Void> validateOperationRequest(
            OperationRequest request,
            String idOperation
    ) {

        Map<String, String> errors = new HashMap<>();


        validateDocument(
                errors,
                "documentType",
                "documentNumber",
                request.documentType(),
                request.documentNumber()
        );


        validateOperation(errors, idOperation);

        if (!errors.isEmpty()) {
            return Mono.error(new BusinessValidationException(errors));
        }

        return Mono.empty();
    }

    private void validateOperation(
            Map<String, String> errors,
            String idOperation
    ) {

        if (idOperation == null || idOperation.isBlank()) {
            errors.put("operation", "Operation is required");
            return;
        }

        String operation = normalize(idOperation);

        if (!List.of(OPERATION_DEPOSIT, OPERATION_WITHDRAW).contains(operation)) {
            errors.put("operation", "Operation must be DEPOSIT or WITHDRAW");
        }
    }

    private void validateParticipants(
            Map<String, String> errors,
            AccountRequest request
    ) {

        List<AccountParticipantRequest> participants = getSafeParticipants(request);

        Set<String> documentNumbers = new HashSet<>();

        if (request.documentNumber() != null && !request.documentNumber().isBlank()) {
            documentNumbers.add(normalizeText(request.documentNumber()));
        }

        for (int i = 0; i < participants.size(); i++) {

            AccountParticipantRequest participant = participants.get(i);

            if (participant == null) {
                errors.put("participants[" + i + "]", "Participant is required");
                continue;
            }

            String prefix = "participants[" + i + "]";

            validateDocument(
                    errors,
                    prefix + ".documentType",
                    prefix + ".documentNumber",
                    participant.documentType(),
                    participant.documentNumber()
            );


            validateDuplicatedParticipantDocument(
                    errors,
                    prefix,
                    documentNumbers,
                    participant.documentNumber()
            );

            validateParticipantRole(
                    errors,
                    prefix + ".participantRole",
                    participant.participantRole()
            );
        }
    }

    private void validateDuplicatedParticipantDocument(
            Map<String, String> errors,
            String prefix,
            Set<String> documentNumbers,
            String documentNumber
    ) {

        if (documentNumber == null || documentNumber.isBlank()) {
            return;
        }

        String cleanDocumentNumber = normalizeText(documentNumber);

        if (!documentNumbers.add(cleanDocumentNumber)) {
            errors.put(
                    prefix + ".documentNumber",
                    "Participant document number must not be duplicated or equal to the main customer document number"
            );
        }
    }

    private void validateDocument(
            Map<String, String> errors,
            String documentTypeField,
            String documentNumberField,
            String documentType,
            String documentNumber
    ) {

        if (documentType == null || documentType.isBlank()) {
            errors.put(documentTypeField, "Document type is required");
            return;
        }

        String cleanDocumentType = normalize(documentType);

        if (!List.of(DOCUMENT_TYPE_PERSONAL, DOCUMENT_TYPE_BUSINESS)
                .contains(cleanDocumentType)) {
            errors.put(
                    documentTypeField,
                    "Document type must be 01 for PERSONAL or 02 for BUSINESS"
            );
            return;
        }

        if (documentNumber == null || documentNumber.isBlank()) {
            errors.put(documentNumberField, "Document number is required");
            return;
        }

        String cleanDocumentNumber = normalizeText(documentNumber);

        if (DOCUMENT_TYPE_PERSONAL.equals(cleanDocumentType)
                && !cleanDocumentNumber.matches("^[0-9]{8}$")) {
            errors.put(
                    documentNumberField,
                    "Personal document number must contain exactly 8 digits"
            );
        }

        if (DOCUMENT_TYPE_BUSINESS.equals(cleanDocumentType)
                && !cleanDocumentNumber.matches("^[0-9]{11}$")) {
            errors.put(
                    documentNumberField,
                    "Business document number must contain exactly 11 digits"
            );
        }
    }


    private void validateParticipantRole(
            Map<String, String> errors,
            String participantRoleField,
            String participantRole
    ) {

        if (participantRole == null || participantRole.isBlank()) {
            errors.put(participantRoleField, "Participant role is required");
            return;
        }

        String cleanRole = normalize(participantRole);

        if (!List.of(ROLE_HOLDER, ROLE_AUTHORIZED_SIGNER).contains(cleanRole)) {
            errors.put(
                    participantRoleField,
                    "Participant role must be HOLDER or AUTHORIZED_SIGNER"
            );
        }
    }

    private List<AccountParticipantRequest> getSafeParticipants(
            AccountRequest request
    ) {
        return request.participants() == null
                ? List.of()
                : request.participants();
    }

    private String getAccountTypeName(String accountType) {
        return switch (accountType) {
            case ACCOUNT_TYPE_SAVINGS -> "SAVINGS";
            case ACCOUNT_TYPE_CHECKING -> "CHECKING";
            case ACCOUNT_TYPE_FIXED_TERM -> "FIXED_TERM";
            default -> "UNKNOWN";
        };
    }

    private String normalize(String value) {
        return value == null
                ? ""
                : value.trim().toUpperCase();
    }

    private String normalizeText(String value) {
        return value == null
                ? ""
                : value.trim();
    }

    private String maskValue(String value) {

        if (value == null || value.isBlank()) {
            return "EMPTY";
        }

        String cleanValue = value.trim();

        if (cleanValue.length() <= 4) {
            return "****";
        }

        return "****" + cleanValue.substring(cleanValue.length() - 4);
    }

    private void logUnexpectedError(String message, Throwable error) {

        if (error instanceof BusinessValidationException
                || error instanceof ResourceNotFoundException) {
            return;
        }

        log.error(message, error);
    }

    private record AccountAccessContext(
            CustomerResponse customer,
            Account account
    ) {
    }

}
