package diarsid.messaging.jdbc.impl;

import java.util.ArrayList;
import java.util.List;

import diarsid.jdbc.api.Jdbc;
import diarsid.messaging.api.Messaging;
import diarsid.messaging.defaultimpl.MessagingThreads;
import diarsid.support.concurrency.stateful.workers.AbstractStatefulDestroyableWorker;
import diarsid.support.concurrency.threads.NamedThreadSource;
import diarsid.support.exceptions.UnsupportedLogicException;

public class MessagingThroughJdbcImpl
        extends AbstractStatefulDestroyableWorker
        implements Messaging, MessagingThreads {

    private final List<Agent> connectedAgents;
    public final Jdbc jdbc;
    final NamedThreadSource namedThreadSource;

    public MessagingThroughJdbcImpl(Jdbc jdbc) {
        this.jdbc = jdbc;
        this.createTablesIfNotExist();
        this.namedThreadSource = new NamedThreadSource("diarsid.jdbc.messaging");
        this.connectedAgents = new ArrayList<>();
        this.startWork();
    }

    private void createTablesIfNotExist() {
        jdbc.doInTransaction((transaction) -> {
            transaction.doUpdate(
                    "CREATE TABLE IF NOT EXISTS jdbc_m_destinations ( \n" +
                    "   name            VARCHAR(255) PRIMARY KEY \n" +
                    ")");

            transaction.doUpdate(
                    "CREATE TABLE IF NOT EXISTS jdbc_m_topics (\n" +
                    "   name            VARCHAR(255) PRIMARY KEY, \n" +
                    "   time            TIMESTAMP(9) WITHOUT TIME ZONE NOT NULL, \n" +
                    "   type            VARCHAR NOT NULL \n" +
                    ")");

            transaction.doUpdate(
                    "ALTER TABLE jdbc_m_topics \n" +
                    "ADD CONSTRAINT IF NOT EXISTS FK_topic_names \n" +
                    "FOREIGN KEY(name) REFERENCES jdbc_m_destinations(name)");

            transaction.doUpdate(
                    "CREATE TABLE IF NOT EXISTS jdbc_m_queues (\n" +
                    "   name            VARCHAR(255) PRIMARY KEY, \n" +
                    "   time            TIMESTAMP(9) WITHOUT TIME ZONE NOT NULL, \n" +
                    "   type            VARCHAR NOT NULL \n" +
                    ")");

            transaction.doUpdate(
                    "ALTER TABLE jdbc_m_queues \n" +
                    "ADD CONSTRAINT IF NOT EXISTS FK_queue_names \n" +
                    "FOREIGN KEY(name) REFERENCES jdbc_m_destinations(name)");

            transaction.doUpdate(
                    "CREATE TABLE IF NOT EXISTS jdbc_m_agents (\n" +
                    "   name            VARCHAR(255) PRIMARY KEY, \n" +
                    "   time            TIMESTAMP(9) WITHOUT TIME ZONE NOT NULL \n" +
                    ")");

            transaction.doUpdate(
                    "CREATE TABLE IF NOT EXISTS jdbc_m_topic_agents (\n" +
                    "   agent_name      VARCHAR(255) PRIMARY KEY, \n" +
                    "   topic_name      VARCHAR(255) NOT NULL, \n" +
                    "   message_offset  BIGINT NOT NULL, \n" +
                    "   message_read    TIMESTAMP(9) WITHOUT TIME ZONE NOT NULL \n" +
                    ")");

            transaction.doUpdate(
                    "ALTER TABLE jdbc_m_topic_agents \n" +
                    "ADD CONSTRAINT IF NOT EXISTS FK_topic_agent_names \n" +
                    "FOREIGN KEY(agent_name) REFERENCES jdbc_m_agents(name)");

            transaction.doUpdate(
                    "ALTER TABLE jdbc_m_topic_agents \n" +
                    "ADD CONSTRAINT IF NOT EXISTS FK_topic_agent_topic_names \n" +
                    "FOREIGN KEY(topic_name) REFERENCES jdbc_m_topics(name)");

            transaction.doUpdate(
                    "CREATE TABLE IF NOT EXISTS jdbc_m_queue_agents (\n" +
                    "   agent_name      VARCHAR(255) PRIMARY KEY, \n" +
                    "   queue_name      VARCHAR(255) NOT NULL, \n" +
                    "   message_offset  BIGINT NOT NULL, \n" +
                    "   message_read    TIMESTAMP(9) WITHOUT TIME ZONE NOT NULL \n" +
                    ")");

            transaction.doUpdate(
                    "ALTER TABLE jdbc_m_queue_agents \n" +
                    "ADD CONSTRAINT IF NOT EXISTS FK_queue_agent_names \n" +
                    "FOREIGN KEY(agent_name) REFERENCES jdbc_m_agents(name)");

            transaction.doUpdate(
                    "ALTER TABLE jdbc_m_queue_agents \n" +
                    "ADD CONSTRAINT IF NOT EXISTS FK_queue_agent_queue_names \n" +
                    "FOREIGN KEY(queue_name) REFERENCES jdbc_m_queues(name)");

        });
    }

    @Override
    protected boolean doSynchronizedStartWork() {
        return true;
    }

    @Override
    protected boolean doSynchronizedDestroy() {
        for ( Agent agent : this.connectedAgents ) {
            agent.destroy();
        }
        this.namedThreadSource.close();

        return true;
    }

    @Override
    public NamedThreadSource threads() {
        return this.namedThreadSource;
    }

    void add(Agent agent) {
        super.doSynchronizedVoidChange(() -> {
            this.connectedAgents.add(agent);
        });
    }

    @Override
    public <T> Queue<T> queue(String name, Class<T> type) {
        throw new UnsupportedLogicException();
    }

    @Override
    public <T> Queue<T> queue(String name, Destination.Serialization<T> serialization) {
        return new TableForQueue<>(this, name, serialization, null);
    }

    @Override
    public <T> Queue<T> queue(String name, Class<T> type, Destination.Cleaning cleaning) {
        throw new UnsupportedLogicException();
    }

    @Override
    public <T> Queue<T> queue(String name, Destination.Serialization<T> serialization, Destination.Cleaning cleaning) {
        return new TableForQueue<>(this, name, serialization, cleaning);
    }

    @Override
    public <T> Topic<T> topic(String name, Class<T> type) {
        throw new UnsupportedLogicException();
    }

    @Override
    public <T> Topic<T> topic(String name, Destination.Serialization<T> serialization) {
        return new TableForTopic<>(this, name, serialization, null);
    }

    @Override
    public <T> Topic<T> topic(String name, Class<T> type, Destination.Cleaning cleaning) {
        throw new UnsupportedLogicException();
    }

    @Override
    public <T> Topic<T> topic(String name, Destination.Serialization<T> serialization, Destination.Cleaning cleaning) {
        return new TableForTopic<>(this, name, serialization, cleaning);
    }
}
