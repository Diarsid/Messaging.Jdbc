package diarsid.messaging.jdbc.impl;

import diarsid.messaging.api.Messaging;
import diarsid.messaging.defaultimpl.DestinationAsyncCleaner;
import diarsid.support.objects.references.Possible;
import diarsid.support.objects.references.References;

import static java.lang.String.format;
import static java.time.LocalDateTime.now;
import static java.util.Objects.isNull;

public abstract class TableForDestination<T> implements Messaging.Destination<T> {

    public static class Namings {

        public static final String TABLES_PREFIX = "jdbc_m_";
        public static final String QUEUE_TABLE_PREFIX = TABLES_PREFIX + "queue_";
        public static final String TOPIC_TABLE_PREFIX = TABLES_PREFIX + "topic_";

        public static String tablePrefixOf(Type type) {
            switch ( type ) {
                case QUEUE: return QUEUE_TABLE_PREFIX;
                case TOPIC: return TOPIC_TABLE_PREFIX;
                default: throw type.unsupported();
            }
        }
    }

    public static final long NO_OFFSET = -1;

    public final MessagingThroughJdbcImpl messaging;
    protected final String name;
    protected final String tableName;
    protected final Serialization<T> serialization;
    private final Possible<DestinationAsyncCleaner> cleaner;
    private final String INSERT_INTO_table_I_authorAgentName_message_created_I_VALUES_I_a1_a2_a3_I;

    public TableForDestination(
            MessagingThroughJdbcImpl messaging,
            Messaging.Destination.Type type,
            String name,
            Serialization<T> serialization,
            Cleaning cleaning) {
        this.messaging = messaging;
        this.name = name;
        this.tableName = Namings.tablePrefixOf(type) + name;
        this.serialization = serialization;

        this.INSERT_INTO_table_I_authorAgentName_message_created_I_VALUES_I_a1_a2_a3_I = format(
                "INSERT INTO %s ( \n" +
                "   author_agent_name, \n" +
                "   message, \n" +
                "   created) \n" +
                "VALUES (?, ?, ?)",
                this.tableName);

        if ( isNull(cleaning) ) {
            this.cleaner = References.simplePossibleButEmpty();
        }
        else {
            this.cleaner = References.simplePossibleWith(new DestinationAsyncCleaner(
                    this.messaging,
                    this,
                    cleaning));
        }
    }

    public abstract void createIfNotExists();

    @Override
    public final void send(T t) {
        Object obj = this.serialization.serialize(t);
        this.messaging.jdbc.doUpdate(
                this.INSERT_INTO_table_I_authorAgentName_message_created_I_VALUES_I_a1_a2_a3_I,
                this.name, obj, now());
    }

    @Override
    public String name() {
        return this.name;
    }

    public String tableName() {
        return this.tableName;
    }

    public Serialization<T> serialization() {
        return this.serialization;
    }
}
