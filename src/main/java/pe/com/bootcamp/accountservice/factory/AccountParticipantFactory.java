package pe.com.bootcamp.accountservice.factory;

import org.springframework.stereotype.Component;
import pe.com.bootcamp.accountservice.model.AccountParticipant;

import java.time.LocalDateTime;

@Component
public class AccountParticipantFactory {

    public AccountParticipant create(
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

    private String normalize(String value) {

        return value == null
                ? ""
                : value.trim().toUpperCase();
    }


}
