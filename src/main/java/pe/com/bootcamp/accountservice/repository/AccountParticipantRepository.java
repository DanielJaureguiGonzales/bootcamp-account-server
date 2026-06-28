package pe.com.bootcamp.accountservice.repository;

import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import pe.com.bootcamp.accountservice.model.AccountParticipant;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository
public interface AccountParticipantRepository extends ReactiveMongoRepository<AccountParticipant, String> {

    Flux<AccountParticipant> findByCustomerIdAndStatus(
            String customerId,
            Boolean status
    );

    Flux<AccountParticipant> findByCustomerIdAndParticipantRoleAndStatus(
            String customerId,
            String participantRole,
            Boolean status
    );

    Mono<Boolean> existsByAccountIdAndCustomerIdAndParticipantRoleInAndStatus(
            String accountId,
            String customerId,
            List<String> participantRoles,
            Boolean status
    );
}

