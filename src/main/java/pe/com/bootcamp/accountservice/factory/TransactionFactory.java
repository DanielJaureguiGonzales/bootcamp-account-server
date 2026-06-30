package pe.com.bootcamp.accountservice.factory;

import org.springframework.stereotype.Component;
import pe.com.bootcamp.accountservice.dto.OperationCompleted;
import pe.com.bootcamp.accountservice.dto.OperationRequest;
import pe.com.bootcamp.accountservice.model.Account;
import pe.com.bootcamp.accountservice.model.Transaction;

import java.time.LocalDateTime;

@Component
public class TransactionFactory {
    public Transaction create(
            OperationRequest request,
            Account account,
            String operation,
            String customerId
    ) {

        return Transaction.builder()
                .accountId(account.getAccountId())
                .accountNumber(account.getAccountNumber())
                .customerId(customerId)
                .documentNumber(request.documentNumber())
                .transactionType(operation)
                .amount(request.amount())
                .transactionDate(LocalDateTime.now())
                .status(true)
                .build();
    }

    public OperationCompleted toOperationCompleted(
            Transaction transaction,
            String operation
    ) {

        return OperationCompleted.builder()
                .accountId(transaction.getAccountId())
                .customerId(transaction.getCustomerId())
                .operation(operation)
                .transactionDate(transaction.getTransactionDate())
                .amount(transaction.getAmount())
                .build();
    }
}
