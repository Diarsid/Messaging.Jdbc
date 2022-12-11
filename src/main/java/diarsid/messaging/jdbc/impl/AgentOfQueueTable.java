package diarsid.messaging.jdbc.impl;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diarsid.messaging.api.Messaging;
import diarsid.messaging.api.exceptions.AgentNotCreatedException;
import diarsid.messaging.api.exceptions.InvalidAgentDataException;
import diarsid.support.objects.references.Possible;
import diarsid.support.objects.references.References;

import static java.lang.String.format;
import static java.time.LocalDateTime.now;
import static java.util.stream.Collectors.toList;

import static diarsid.messaging.api.Messaging.Destination.Type.QUEUE;
import static diarsid.messaging.jdbc.impl.TableForDestination.NO_OFFSET;

public class AgentOfQueueTable<T> extends AgentOfTable implements Messaging.Agent.OfQueue<T> {

    private static final Logger log = LoggerFactory.getLogger(AgentOfQueueTable.class);

    private final TableForQueue<T> queue;
    private final String queueTableName;
    private final Messaging.Receiver<T> receiver;
    private final Possible<String> bufferedReceiverName;
    private final Possible<Object> bufferedMessage;

    private final String SELECT_all_FROM_queueTableName_WHERE_receiverAgentName_IS_NULL_AND_authorAgentName_not_a1;
    private final String SELECT_all_FROM_queueTableName_WHERE_offset_a1_FOR_UPDATE;
    private final String UPDATE_queueTableName_SET_receiverAgentName_a1_read_a2_WHERE_offset_a3;

    public AgentOfQueueTable(
            MessagingThroughJdbcImpl jdbcMessaging,
            String agentName,
            TableForQueue<T> queue,
            Messaging.Receiver<T> receiver) {
        super(jdbcMessaging, agentName, QUEUE, queue.name());
        this.queue = queue;
        this.queueTableName = this.queue.tableName();
        this.receiver = receiver;

        this.bufferedMessage = References.simplePossibleButEmpty();
        this.bufferedReceiverName = References.simplePossibleButEmpty();

        this.queue.createIfNotExists();

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

            boolean isTopicAgent = transaction
                    .countQueryResults(
                            "SELECT * \n" +
                            "FROM jdbc_m_topic_agents \n" +
                            "WHERE agent_name = ?",
                            this.name()) > 0;

            if ( isTopicAgent ) {
                throw new AgentNotCreatedException();
            }

            Optional<Long> savedOffset = transaction
                    .doQueryAndConvertFirstRow(
                            row -> row.longOf("message_offset"),
                            "SELECT * \n" +
                            "FROM jdbc_m_queue_agents \n" +
                            "WHERE \n" +
                            "   agent_name = ? AND \n" +
                            "   queue_name = ? \n" +
                            "FOR UPDATE",
                            this.name(), this.queue.name());

            if ( savedOffset.isEmpty() ) {
                transaction
                        .doUpdate(
                                "INSERT INTO jdbc_m_queue_agents ( \n" +
                                "   agent_name, \n" +
                                "   queue_name, \n" +
                                "   message_offset, \n" +
                                "   message_read) \n" +
                                "VALUES (?, ?, ?, ?)",
                                this.name(), this.queue.name(), NO_OFFSET, now());
            }
        });

        this.SELECT_all_FROM_queueTableName_WHERE_receiverAgentName_IS_NULL_AND_authorAgentName_not_a1 = format(
                "SELECT * \n " +
                "FROM %s \n " +
                "WHERE \n " +
                "   receiver_agent_name IS NULL AND \n" +
                "   author_agent_name != ? \n" +
                "ORDER BY message_offset ASC",
                this.queueTableName);

        this.SELECT_all_FROM_queueTableName_WHERE_offset_a1_FOR_UPDATE = format(
                "SELECT * \n " +
                "FROM %s \n " +
                "WHERE message_offset = ? \n " +
                "FOR UPDATE",
                this.queueTableName);

        this.UPDATE_queueTableName_SET_receiverAgentName_a1_read_a2_WHERE_offset_a3 = format(
                "UPDATE %s \n" +
                "SET \n" +
                "   receiver_agent_name = ?, \n" +
                "   read = ? \n" +
                "WHERE message_offset = ?",
                this.queueTableName);
    }

    @Override
    protected void doScheduledLogic() {
        List<Long> newOffsets = this.jdbc
                .doQueryAndStream(
                        row -> row.longOf("message_offset"),
                        SELECT_all_FROM_queueTableName_WHERE_receiverAgentName_IS_NULL_AND_authorAgentName_not_a1,
                        super.name())
                .collect(toList());

        if ( newOffsets.isEmpty() ) {
            return;
        }

        for ( Long offset : newOffsets ) {
            try {
                this.jdbc.doInTransaction((transaction) -> {
                    transaction
                            .doQueryAndProcessFirstRow(
                                    (row) -> {
                                        this.bufferedReceiverName.resetTo(row.stringOf("receiver_agent_name"));
                                        this.bufferedMessage.resetTo(row.get("message"));
                                    },
                                    SELECT_all_FROM_queueTableName_WHERE_offset_a1_FOR_UPDATE,
                                    offset);

                    if ( this.bufferedReceiverName.isPresent() ) {
                        return;
                    }

                    LocalDateTime read = now();
                    this.receiver.receive(offset, this.queue.serialization().deserialize(this.bufferedMessage.get()));

                    int updatedQueue = transaction
                            .doUpdate(
                                    UPDATE_queueTableName_SET_receiverAgentName_a1_read_a2_WHERE_offset_a3,
                                    super.name(), read, offset);

                    int updatedAgent = transaction
                            .doUpdate(
                                    "UPDATE jdbc_m_queue_agents " +
                                    "SET " +
                                    "   message_offset = ?, " +
                                    "   message_read = ? " +
                                    "WHERE " +
                                    "   agent_name = ? AND " +
                                    "   queue_name = ?",
                                    offset, read, this.name(), this.queue.name());

                    if ( updatedQueue != 1 || updatedAgent != 1) {
                        throw new IllegalStateException();
                    }
                });
            }
            finally {
                this.bufferedMessage.nullify();
                this.bufferedReceiverName.nullify();
            }
        }
    }

    @Override
    public Messaging.Queue<T> queue() {
        return this.queue;    }
}
