package pe.com.bootcamp.accountservice.service.client;

import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.service.annotation.GetExchange;
import org.springframework.web.service.annotation.PostExchange;
import pe.com.bootcamp.accountservice.dto.CustomerResponse;
import pe.com.bootcamp.accountservice.dto.CustomerSummaryResponse;
import pe.com.bootcamp.accountservice.dto.DocumentNumbersRequest;
import reactor.core.publisher.Mono;

import java.util.List;

public interface CustomerClient {

/*
    @GetExchange("/customers/documentNumber/{documentNumber}")
    Mono<ResponseEntity<CustomerResponse>> findByDocumentNumber(@PathVariable String documentNumber);
*/

    @PostExchange("/internal/customers/document-numbers/search")
    Mono<List<CustomerSummaryResponse>> searchCustomerReponse(@RequestBody @Valid DocumentNumbersRequest documentNumbersRequests);

    @GetExchange("/internal/customers/document-number")
    Mono<CustomerResponse> getCustomerResponse(
            @RequestParam String documentNumber,
            @RequestParam String documentType
    );
}
