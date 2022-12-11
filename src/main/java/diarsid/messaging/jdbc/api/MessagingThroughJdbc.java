package diarsid.messaging.jdbc.api;

import diarsid.jdbc.api.Jdbc;
import diarsid.messaging.jdbc.impl.MessagingThroughJdbcImpl;
import diarsid.messaging.api.Messaging;

public interface MessagingThroughJdbc {

    static Messaging jdbcMessaging(Jdbc jdbc) {
        return new MessagingThroughJdbcImpl(jdbc);
    }
}
