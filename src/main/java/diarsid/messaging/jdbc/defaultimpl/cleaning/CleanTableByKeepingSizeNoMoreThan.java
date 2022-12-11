package diarsid.messaging.jdbc.defaultimpl.cleaning;

import java.util.concurrent.TimeUnit;

import diarsid.jdbc.api.Jdbc;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.api.exceptions.CleaningException;
import diarsid.messaging.jdbc.impl.TableForDestination;

public class CleanTableByKeepingSizeNoMoreThan extends CleanTable {

    private final long threshold;

    public CleanTableByKeepingSizeNoMoreThan(int interval, TimeUnit unit, long threshold) {
        super(interval, unit);
        this.threshold = threshold;
    }

    @Override
    public void clean(Messaging.Destination<?> destination) {
        TableForDestination<?> tableForDestination = (TableForDestination<?>) destination;
        String tableName = tableForDestination.tableName();

        Jdbc jdbc = tableForDestination.messaging.jdbc;

        jdbc.doInTransaction((transaction) -> {
            long total = transaction
                    .doQueryAndConvertFirstRow(
                            row -> row.longOf("total"),
                            "SELECT COUNT(*) AS total \n" +
                            "FROM " + tableName)
                    .orElseThrow(() -> new CleaningException("Cannot get total of " + tableName));

            if ( total < this.threshold ) {
                return;
            }

            long nToRemove = total - this.threshold;

            long minOffset = transaction
                    .doQueryAndConvertFirstRow(
                            row -> row.longOf("minOffset"),
                            "SELECT MIN(message_offset) AS minOffset \n" +
                            "FROM " + tableName)
                    .orElseThrow(() -> new CleaningException("Cannot get min message offset of " + tableName));

            long maxOffsetToRemove = nToRemove + minOffset;

            transaction
                    .doUpdate(
                            "DELETE FROM " + tableName + " \n" +
                            "WHERE \n" +
                            "   message_offset > ? AND \n" +
                            "   message_offset < ?",
                            minOffset - 1, maxOffsetToRemove + 1);
        });
    }
}
