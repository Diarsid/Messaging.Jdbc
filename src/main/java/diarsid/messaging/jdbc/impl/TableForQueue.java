package diarsid.messaging.jdbc.impl;

import java.util.Optional;

import diarsid.messaging.api.Messaging;

import static java.lang.String.format;
import static java.time.LocalDateTime.now;

import static diarsid.messaging.api.Messaging.Destination.Type.QUEUE;

class TableForQueue<T> extends TableForDestination<T> implements Messaging.Queue<T> {

    TableForQueue(
            MessagingThroughJdbcImpl jdbcMessaging,
            String name,
            Serialization<T> serialization,
            Cleaning cleaning) {
        super(jdbcMessaging, QUEUE, name, serialization, cleaning);
        createIfNotExists();
    }

    @Override
    public void createIfNotExists() {
        String messageDataType;
        Serialization.PersistentType persistentType = super.serialization.persistentType();
        switch ( persistentType ) {
            case STRING: messageDataType = "VARCHAR"; break;
            case BYTES: messageDataType = "VARBINARY"; break;
            default: throw persistentType.unsupported();
        }

        super.messaging.jdbc.doInTransaction(transaction -> {

            Optional<Serialization.PersistentType> storedPersistentType = transaction.doQueryAndConvertFirstRow(
                    row -> Serialization.PersistentType.valueOf(row.stringOf("type")),
                    "SELECT type \n" +
                    "FROM jdbc_m_queues \n" +
                    "WHERE name = ?",
                    super.name);

            if ( storedPersistentType.isEmpty() ) {
                transaction.doUpdate(
                        "INSERT INTO jdbc_m_destinations (name) \n" +
                        "VALUES(?)",
                        super.name);

                transaction.doUpdate(
                        "INSERT INTO jdbc_m_queues (name, type, time) \n" +
                        "VALUES(?, ?, ?)",
                        super.name, persistentType, now());
            }
            else {
                storedPersistentType.get().mustBe(persistentType);
            }

            transaction.doUpdate(format(
                    "CREATE TABLE IF NOT EXISTS %s ( \n" +
                    "   message_offset      BIGINT AUTO_INCREMENT PRIMARY KEY, \n" +
                    "   message             %s NOT NULL, \n" +
                    "   author_agent_name   VARCHAR(255) NOT NULL, \n" +
                    "   receiver_agent_name VARCHAR(255), \n" +
                    "   created             TIMESTAMP(9) WITHOUT TIME ZONE NOT NULL,\n" +
                    "   read                TIMESTAMP(9) WITHOUT TIME ZONE )",
                    super.tableName, messageDataType));
        });
    }

    @Override
    public Messaging.Agent.OfQueue<T> connect(String agentName, Messaging.Receiver<T> receiver) {
        var agent = new AgentOfQueueTable<>(super.messaging, agentName, this, receiver);
        this.messaging.add(agent);
        return agent;
    }
}
