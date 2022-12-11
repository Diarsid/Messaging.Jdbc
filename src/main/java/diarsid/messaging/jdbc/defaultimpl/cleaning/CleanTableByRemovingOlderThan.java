package diarsid.messaging.jdbc.defaultimpl.cleaning;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import diarsid.jdbc.api.Jdbc;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.jdbc.impl.TableForDestination;

import static java.time.LocalDateTime.now;

public class CleanTableByRemovingOlderThan extends CleanTable {

    private final Duration duration;

    public CleanTableByRemovingOlderThan(int interval, TimeUnit unit, Duration duration) {
        super(interval, unit);
        this.duration = duration;
    }

    @Override
    public void clean(Messaging.Destination<?> destination) {
        TableForDestination<?> tableForDestination = (TableForDestination<?>) destination;
        String tableName = tableForDestination.tableName();

        Jdbc jdbc = tableForDestination.messaging.jdbc;

        jdbc.doInTransaction((transaction) -> {
            LocalDateTime now = now();
            LocalDateTime threshold = now.minus(this.duration);

            int removed = transaction
                    .doUpdate(
                            "DELETE FROM " + tableName + " \n" +
                            "WHERE created < ?", threshold);
        });
    }
}
