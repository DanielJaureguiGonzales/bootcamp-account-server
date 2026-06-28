package pe.com.bootcamp.accountservice.constants;

import java.time.format.DateTimeFormatter;

public class AccountConstants {

    private AccountConstants() {
    }

    public static final String DOCUMENT_TYPE_PERSONAL = "01";
    public static final String DOCUMENT_TYPE_BUSINESS = "02";

    public static final String ACCOUNT_TYPE_SAVINGS = "01";
    public static final String ACCOUNT_TYPE_CHECKING = "02";
    public static final String ACCOUNT_TYPE_FIXED_TERM = "03";

    public static final String ROLE_HOLDER = "HOLDER";
    public static final String ROLE_AUTHORIZED_SIGNER = "AUTHORIZED_SIGNER";

    public static final String OPERATION_DEPOSIT = "DEPOSIT";
    public static final String OPERATION_WITHDRAW = "WITHDRAW";
    public static final String CURRENCY_TYPE_PEN = "PEN";
    public static final String CURRENCY_NAME_SOLES = "SOLES";
    public static final DateTimeFormatter FIXED_TERM_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("dd/MM/yyyy");
}
