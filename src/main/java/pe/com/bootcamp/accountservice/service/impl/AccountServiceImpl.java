package pe.com.bootcamp.accountservice.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import pe.com.bootcamp.accountservice.dto.*;
import pe.com.bootcamp.accountservice.exceptions.BusinessValidationException;
import pe.com.bootcamp.accountservice.exceptions.ResourceNotFoundException;
import pe.com.bootcamp.accountservice.generator.AccountNumberGenerator;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
    private final AccountNumberGenerator accountNumberGenerator;


    @Override
    public Mono<AccountResponse> createAccountCustomer(AccountRequest accountRequest) {

        return validateAccountRequest(accountRequest)
                .then(client.getCustomerResponse(accountRequest))
                .doOnNext(customerResponse ->
                        log.info("Petición de cliente recibido {}", customerResponse)
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
                .map(this::toAccountResponse);
    }

    @Override
    public Mono<OperationCompleted> transactions(
            OperationRequest operationRequest,
            String idOperation
    ) {

        return validateOperationRequest(operationRequest, idOperation)
                .then(client.getCustomerResponseByCustomer(
                        operationRequest.documentNumber(),
                        operationRequest.documentType()
                ))
                .flatMap(customerResponse -> {

                    String operation = normalize(idOperation);

                    return accountRepository
                            .findByAccountNumberAndStatus(
                                    operationRequest.accountNumber(),
                                    true
                            )
                            .switchIfEmpty(Mono.error(new ResourceNotFoundException(
                                    "Account",
                                    "accountNumber",
                                    operationRequest.accountNumber()
                            )))
                            .flatMap(account ->
                                    validateCustomerCanOperateAccount(
                                            account,
                                            customerResponse.id()
                                    ).thenReturn(account)
                            )
                            .flatMap(account -> switch (operation) {
                                case OPERATION_DEPOSIT -> depositOperation(
                                        operationRequest,
                                        account,
                                        operation,
                                        customerResponse.id()
                                );

                                case OPERATION_WITHDRAW -> withDrawOperation(
                                        operationRequest,
                                        account,
                                        operation,
                                        customerResponse.id()
                                );

                                default -> Mono.error(new RuntimeException(
                                        "Invalid Operation Id " + idOperation
                                ));
                            });
                });
    }

    @Override
    public Flux<AccountResponse> getAccountByDocumentNumber(
            String documentNumber,
            String documentType
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
            return Flux.error(new BusinessValidationException(errors));
        }

        return client.getCustomerResponseByCustomer(documentNumber, documentType)
                .flatMapMany(customerResponse ->
                        findActiveAccountsByCustomerId(customerResponse.id())
                )
                .map(this::toAccountResponse);
    }

    @Override
    public Mono<AccountBalancesResponse> getAccountBalances(BalanceRequest request) {

        return validateBalanceRequest(request)
                .then(client.getCustomerResponseByCustomer(
                        request.documentNumber(),
                        request.documentType()
                ))
                .flatMap(customerResponse ->
                        findActiveAccountsByCustomerId(customerResponse.id())
                                .map(this::toBankAccountBalanceResponse)
                                .collectList()
                                .map(accounts -> new AccountBalancesResponse(
                                        customerResponse.id(),
                                        request.documentType(),
                                        request.documentNumber(),
                                        accounts
                                ))
                );
    }

    @Override
    public Mono<AccountTransactionsResponse> getAccountTransactions(
            AccountTransactionsRequest request
    ) {

        return validateAccountTransactionsRequest(request)
                .then(client.getCustomerResponseByCustomer(
                        request.documentNumber(),
                        request.documentType()
                ))
                .flatMap(customerResponse ->
                        accountRepository
                                .findByAccountNumberAndStatus(
                                        request.accountNumber(),
                                        true
                                )
                                .switchIfEmpty(Mono.error(new ResourceNotFoundException(
                                        "Account",
                                        "accountNumber",
                                        request.accountNumber()
                                )))
                                .flatMap(account ->
                                        validateCustomerCanViewAccountTransactions(
                                                account,
                                                customerResponse.id()
                                        ).thenReturn(account)
                                )
                                .flatMap(account ->
                                        transactionsRepository
                                                .findByAccountIdAndStatusOrderByTransactionDateDesc(
                                                        account.getAccountId(),
                                                        true
                                                )
                                                .map(this::toTransactionResponse)
                                                .collectList()
                                                .map(transactions -> new
                                                        AccountTransactionsResponse(
                                                            customerResponse.id(),
                                                            request.documentType(),
                                                            request.documentNumber(),
                                                            account.getAccountNumber(),
                                                            account.getAccountType(),
                                                            account.getBalance(),
                                                            transactions
                                                ))
                                )
                );
    }

    private Mono<OperationCompleted> withDrawOperation(
            OperationRequest operationRequest,
            Account account,
            String operation,
            String customerId
    ) {

        BigDecimal currentBalance = account.getBalance() == null
                ? BigDecimal.ZERO
                : account.getBalance();

        if (operationRequest.amount().compareTo(currentBalance) > 0) {
            return Mono.error(new RuntimeException("Insufficient balance"));
        }

        account.setBalance(currentBalance.subtract(operationRequest.amount()));

        return saveTransaction(
                operationRequest,
                account,
                operation,
                customerId
        );
    }

    private Mono<OperationCompleted> depositOperation(
            OperationRequest operationRequest,
            Account account,
            String operation,
            String customerId
    ) {

        BigDecimal currentBalance = account.getBalance() == null
                ? BigDecimal.ZERO
                : account.getBalance();

        account.setBalance(currentBalance.add(operationRequest.amount()));

        return saveTransaction(
                operationRequest,
                account,
                operation,
                customerId
        );
    }

    private Mono<OperationCompleted> saveTransaction(
            OperationRequest operationRequest,
            Account account,
            String operation,
            String customerId
    ) {

        return accountRepository.save(account)
                .flatMap(accountSaved -> {
                    Transaction transaction = buildTransaction(
                            operationRequest,
                            accountSaved,
                            operation,
                            customerId
                    );

                    return transactionsRepository.save(transaction);
                })
                .map(transaction -> OperationCompleted.builder()
                        .accountId(transaction.getAccountId())
                        .customerId(transaction.getCustomerId())
                        .operation(operation)
                        .transactionDate(transaction.getTransactionDate())
                        .amount(transaction.getAmount())
                        .build()
                );
    }

    private Transaction buildTransaction(
            OperationRequest operationRequest,
            Account account,
            String operation,
            String customerId
    ) {

        return Transaction.builder()
                .accountId(account.getAccountId())
                .accountNumber(account.getAccountNumber())
                .customerId(customerId)
                .documentNumber(operationRequest.documentNumber())
                .transactionType(operation)
                .amount(operationRequest.amount())
                .transactionDate(LocalDateTime.now())
                .status(true)
                .build();
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

    private Mono<Account> createAccountAndParticipant(
            CustomerResponse customer,
            AccountRequest accountRequest,
            AccountValidationResult validationResult
    ) {

        Account account = Account.builder()
                .accountNumber(accountNumberGenerator.generate())
                .accountType(normalize(accountRequest.accountType()))
                .balance(accountRequest.initialAmount())
                .status(true)
                .openingDate(LocalDateTime.now())
                .flagFreeCommisionMant(accountRequest.flagFreeCommisionMant())
                .maxMovMon(accountRequest.maxMovMon())
                .initialDate(resolveFixedTermInitialDate(accountRequest))
                .cantDays(accountRequest.cantDays())
                .build();

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

    private LocalDateTime resolveFixedTermInitialDate(AccountRequest accountRequest) {

        if (!ACCOUNT_TYPE_FIXED_TERM.equals(normalize(accountRequest.accountType()))) {
            return null;
        }

        LocalDate date = LocalDate.parse(
                accountRequest.initialDate(),
                FIXED_TERM_DATE_FORMATTER
        );

        return date.atStartOfDay();
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

        AccountParticipant participant = buildParticipant(
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

        AccountParticipant mainHolder = buildParticipant(
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

            AccountParticipant participant = buildParticipant(
                    savedAccount.getAccountId(),
                    participantCustomer.id(),
                    normalize(participantRequest.participantRole())
            );

            participantsToSave.add(participant);
        }

        return accountParticipantRepository.saveAll(participantsToSave).then();
    }

    private AccountParticipant buildParticipant(
            String accountId,
            String customerId,
            String participantRole
    ) {

        AccountParticipant participant = new AccountParticipant();
        participant.setAccountId(accountId);
        participant.setCustomerId(customerId);
        participant.setParticipantRole(normalize(participantRole));
        participant.setRegistrationDate(LocalDateTime.now());
        participant.setStatus(true);

        return participant;
    }

    private Mono<Void> validateCustomerCanOperateAccount(
            Account account,
            String customerId
    ) {

        return accountParticipantRepository
                .existsByAccountIdAndCustomerIdAndParticipantRoleInAndStatus(
                        account.getAccountId(),
                        customerId,
                        List.of(ROLE_HOLDER, ROLE_AUTHORIZED_SIGNER),
                        true
                )
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new RuntimeException(
                        "Customer is not allowed to operate this account"
                )))
                .then();
    }

    private Mono<Void> validateCustomerCanViewAccountTransactions(
            Account account,
            String customerId
    ) {

        return accountParticipantRepository
                .existsByAccountIdAndCustomerIdAndParticipantRoleInAndStatus(
                        account.getAccountId(),
                        customerId,
                        List.of(ROLE_HOLDER, ROLE_AUTHORIZED_SIGNER),
                        true
                )
                .filter(Boolean::booleanValue)
                .switchIfEmpty(Mono.error(new RuntimeException(
                        "Customer is not allowed to view transactions of this account"
                )))
                .then();
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

        if (request == null) {
            errors.put("request", "Account request is required");
            return Mono.error(new BusinessValidationException(errors));
        }

        validateDocument(
                errors,
                "documentType",
                "documentNumber",
                request.documentType(),
                request.documentNumber()
        );

        validateAccountType(errors, request.accountType());
        validateInitialAmount(errors, request.initialAmount());
        /*validateFixedTermDate(errors, request);*/

        validateParticipants(errors, request);

        if (!errors.isEmpty()) {
            return Mono.error(new BusinessValidationException(errors));
        }

        return Mono.empty();
    }

    private Mono<Void> validateBalanceRequest(BalanceRequest request) {

        Map<String, String> errors = new HashMap<>();

        if (request == null) {
            errors.put("request", "Balance request is required");
            return Mono.error(new BusinessValidationException(errors));
        }

        validateDocument(
                errors,
                "documentType",
                "documentNumber",
                request.documentType(),
                request.documentNumber()
        );

        if (!errors.isEmpty()) {
            return Mono.error(new BusinessValidationException(errors));
        }

        return Mono.empty();
    }

    private Mono<Void> validateAccountTransactionsRequest(
            AccountTransactionsRequest request
    ) {

        Map<String, String> errors = new HashMap<>();

        if (request == null) {
            errors.put("request", "Account transactions request is required");
            return Mono.error(new BusinessValidationException(errors));
        }

        validateDocument(
                errors,
                "documentType",
                "documentNumber",
                request.documentType(),
                request.documentNumber()
        );

        if (request.accountNumber() == null || request.accountNumber().isBlank()) {
            errors.put("accountNumber", "Account number is required");
        }

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

        if (request == null) {
            errors.put("request", "Operation request is required");
            return Mono.error(new BusinessValidationException(errors));
        }

        validateDocument(
                errors,
                "documentType",
                "documentNumber",
                request.documentType(),
                request.documentNumber()
        );

        if (request.accountNumber() == null || request.accountNumber().isBlank()) {
            errors.put("accountNumber", "Account number is required");
        }

        if (request.amount() == null) {
            errors.put("amount", "Amount is required");
        } else if (request.amount().compareTo(BigDecimal.ZERO) <= 0) {
            errors.put("amount", "Amount must be greater than zero");
        }

        if (idOperation == null || idOperation.isBlank()) {
            errors.put("operation", "Operation is required");
        } else {
            String operation = normalize(idOperation);

            if (!List.of(OPERATION_DEPOSIT, OPERATION_WITHDRAW).contains(operation)) {
                errors.put("operation", "Operation must be DEPOSIT or WITHDRAW");
            }
        }

        if (!errors.isEmpty()) {
            return Mono.error(new BusinessValidationException(errors));
        }

        return Mono.empty();
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

            validateParticipantRole(
                    errors,
                    prefix + ".participantRole",
                    participant.participantRole()
            );

            if (participant.documentNumber() != null
                    && !participant.documentNumber().isBlank()) {

                String participantDocumentNumber =
                        normalizeText(participant.documentNumber());

                if (!documentNumbers.add(participantDocumentNumber)) {
                    errors.put(
                            prefix + ".documentNumber",
                            "Participant document number must not be duplicated or equal to the main customer document number"
                    );
                }
            }
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

    private void validateAccountType(
            Map<String, String> errors,
            String accountType
    ) {

        if (accountType == null || accountType.isBlank()) {
            errors.put("accountType", "Account type is required");
            return;
        }

        String cleanAccountType = normalize(accountType);

        if (!List.of(
                ACCOUNT_TYPE_SAVINGS,
                ACCOUNT_TYPE_CHECKING,
                ACCOUNT_TYPE_FIXED_TERM
        ).contains(cleanAccountType)) {
            errors.put(
                    "accountType",
                    "Account type must be 01 for SAVINGS, 02 for CHECKING or 03 for FIXED_TERM"
            );
        }
    }

    private void validateInitialAmount(
            Map<String, String> errors,
            BigDecimal initialAmount
    ) {

        if (initialAmount == null) {
            errors.put("initialAmount", "Initial amount is required");
            return;
        }

        if (initialAmount.compareTo(BigDecimal.ZERO) < 0) {
            errors.put("initialAmount", "Initial amount cannot be negative");
        }
    }

    /*private void validateFixedTermDate(
            Map<String, String> errors,
            AccountRequest request
    ) {

        if (!ACCOUNT_TYPE_FIXED_TERM.equals(normalize(request.accountType()))) {
            return;
        }

        if (request.initialDate() == null || request.initialDate().isBlank()) {
            errors.put(
                    "initialDate",
                    "Initial date is required for fixed term accounts"
            );
            return;
        }

        try {
            LocalDate.parse(request.initialDate(), FIXED_TERM_DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            errors.put(
                    "initialDate",
                    "Initial date must have format dd/MM/yyyy"
            );
        }
    }*/

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

    private AccountResponse toAccountResponse(Account account) {

        return AccountResponse.builder()
                .accountId(account.getAccountId())
                .accountNumber(account.getAccountNumber())
                .accountType(account.getAccountType())
                .balance(account.getBalance())
                .status(account.getStatus())
                .build();
    }

    private BankAccountBalanceResponse toBankAccountBalanceResponse(Account account) {

        return new BankAccountBalanceResponse(
                account.getAccountNumber(),
                account.getAccountType(),
                account.getBalance(),
                CURRENCY_TYPE_PEN,
                CURRENCY_NAME_SOLES,
                account.getStatus()
        );
    }

    private TransactionResponse toTransactionResponse(Transaction transaction) {

        return new TransactionResponse(
                transaction.getTransactionId(),
                transaction.getAccountNumber(),
                transaction.getDocumentNumber(),
                transaction.getTransactionType(),
                transaction.getAmount(),
                transaction.getTransactionDate(),
                transaction.getStatus()
        );
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


}
