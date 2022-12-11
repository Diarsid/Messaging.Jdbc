package diarsid.messaging.jdbc.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import diarsid.messaging.api.Messaging;
import diarsid.messaging.api.exceptions.AgentNotCreatedException;
import diarsid.messaging.api.exceptions.InvalidAgentDataException;
import diarsid.support.objects.GuardedPool;
import diarsid.support.objects.PooledReusable;

import static java.lang.String.format;
import static java.time.LocalDateTime.now;

import static diarsid.messaging.api.Messaging.Destination.Type.TOPIC;
import static diarsid.messaging.jdbc.impl.TableForDestination.NO_OFFSET;

public class AgentOfTopicTable<T> extends AgentOfTable implements Messaging.Agent.OfTopic<T> {

    private static final class MessageAndOffset extends PooledReusable {

        Long offset;
        Object message;

        @Override
        protected void clearForReuse() {
            this.offset = null;
            this.message = null;
        }
    }

    private final TableForTopic<T> topic;
    private final Messaging.Receiver<T> receiver;
    private final String topicTableName;
    private final AtomicLong currentOffset;
    private final List<MessageAndOffset> currentNewMessages;
    private final GuardedPool<MessageAndOffset> pool;
    private final String SELECT_all_FROM_topicTableName_WHERE_offset_higher_than_a1_ORDER_BY_offset_ASC;
    private final String SELECT_MAX_OFFSET_FROM_topicTableName;

    public AgentOfTopicTable(
            MessagingThroughJdbcImpl jdbcMessaging,
            String agentName,
            TableForTopic<T> topic,
            Messaging.Receiver<T> receiver) {
        super(jdbcMessaging, agentName, TOPIC, topic.name());
        this.topic = topic;
        this.receiver = receiver;
        this.topicTableName = this.topic.tableName();
        this.currentOffset = new AtomicLong(NO_OFFSET);
        this.currentNewMessages = new ArrayList<>();
        this.pool = new GuardedPool<>(MessageAndOffset::new);

        this.SELECT_all_FROM_topicTableName_WHERE_offset_higher_than_a1_ORDER_BY_offset_ASC = format(
                "SELECT * \n" +
                "FROM %s \n" +
                "WHERE message_offset > ? \n" +
                "ORDER BY message_offset ASC",
                this.topicTableName);

        this.SELECT_MAX_OFFSET_FROM_topicTableName = format(
                "SELECT MAX(message_offset) AS max_offset \n" +
                "FROM %s ",
                this.topicTableName);

        this.topic.createIfNotExists();

        super.jdbc.doInTransaction(transaction -> {
            Optional<String> name = transaction
                    .doQueryAndConvertFirstRow(
                            row -> row.stringOf("name"),
                            "SELECT * \n" +
                            "FROM jdbc_m_agents \n" +
                            "WHERE name = ? \n" +
                            "FOR UPDATE",
                            this.name());

            if ( name.isEmpty() ) {
                throw new InvalidAgentDataException();
            }

            boolean isQueueAgent = transaction
                    .countQueryResults(
                            "SELECT * \n" +
                            "FROM jdbc_m_queue_agents \n" +
                            "WHERE agent_name = ?",
                            this.name()) > 0;

            if ( isQueueAgent ) {
                throw new AgentNotCreatedException();
            }

            Optional<Long> savedOffset = transaction
                    .doQueryAndConvertFirstRow(
                            row -> row.longOf("message_offset"),
                            "SELECT * \n" +
                            "FROM jdbc_m_topic_agents \n" +
                            "WHERE \n" +
                            "   agent_name = ? AND \n" +
                            "   topic_name = ? \n" +
                            "FOR UPDATE",
                            this.name(), this.topic.name());

            if ( savedOffset.isEmpty() ) {
                Optional<Long> existingOffset = transaction
                        .doQueryAndConvertFirstRow(
                                row -> row.longOf("max_offset"),
                                this.SELECT_MAX_OFFSET_FROM_topicTableName);

                this.currentOffset.set(existingOffset.orElse(NO_OFFSET));

                transaction
                        .doUpdate(
                                "INSERT INTO jdbc_m_topic_agents ( \n" +
                                "   agent_name, \n" +
                                "   topic_name, \n" +
                                "   message_offset, \n" +
                                "   message_read) \n" +
                                "VALUES (?, ?, ?, ?)",
                                this.name(), this.topic.name(), this.currentOffset.get(), now());
            }
            else {
                this.currentOffset.set(savedOffset.get());
            }
        });
    }

    @Override
    protected void doScheduledLogic() {
        try {
            this.jdbc
                    .doQuery(
                            row -> {
                                MessageAndOffset messageAndOffset = this.pool.give();
                                messageAndOffset.message = row.get("message");
                                messageAndOffset.offset = row.longOf("message_offset");
                                this.currentNewMessages.add(messageAndOffset);
                            },
                            SELECT_all_FROM_topicTableName_WHERE_offset_higher_than_a1_ORDER_BY_offset_ASC,
                            this.currentOffset.get());

            if ( this.currentNewMessages.isEmpty() ) {
                return;
            }

            for ( MessageAndOffset messageAndOffset : this.currentNewMessages ) {
                this.jdbc.doInTransaction(transaction -> {

                    T t = this.topic.serialization.deserialize(messageAndOffset.message);
                    Long offset = messageAndOffset.offset;
                    this.receiver.receive(offset, t);

                    int updatedAgent = transaction
                            .doUpdate(
                                    "UPDATE jdbc_m_topic_agents " +
                                    "SET " +
                                    "   message_offset = ?, " +
                                    "   message_read = ? " +
                                    "WHERE " +
                                    "   agent_name = ? AND " +
                                    "   topic_name = ?",
                                    offset, now(), this.name(), this.topic.name());

                    if ( updatedAgent != 1) {
                        throw new IllegalStateException();
                    }

                    this.currentOffset.set(offset);
                });
            }
        }
        finally {
            this.pool.takeBackAll(this.currentNewMessages);
            this.currentNewMessages.clear();
        }
    }

    @Override
    public Messaging.Topic<T> topic() {
        return this.topic;
    }
}
