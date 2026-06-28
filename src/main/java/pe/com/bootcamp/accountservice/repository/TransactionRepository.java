package pe.com.bootcamp.accountservice.repository;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import pe.com.bootcamp.accountservice.model.Transaction;
import reactor.core.publisher.Flux;

@Repository
public interface TransactionRepository extends ReactiveMongoRepository<Transaction, String> {
    Flux<Transaction> findByAccountIdAndStatusOrderByTransactionDateDesc(
            String accountId,
            Boolean status
    );
}
