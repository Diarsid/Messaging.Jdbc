package diarsid.messaging.jdbc.impl;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diarsid.jdbc.api.Jdbc;
import diarsid.messaging.api.Messaging;
import diarsid.support.concurrency.stateful.workers.AbstractStatefulPausableDestroyableWorker;
import diarsid.support.concurrency.threads.ThreadsUtil;
import diarsid.support.objects.references.Possible;
import diarsid.support.objects.references.References;

import static java.time.LocalDateTime.now;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

abstract class AgentOfTable
        extends AbstractStatefulPausableDestroyableWorker
        implements Messaging.Agent {

    private static final Logger log = LoggerFactory.getLogger(Messaging.Agent.class);

    protected final MessagingThroughJdbcImpl jdbcMessaging;
    protected final Jdbc jdbc;
    private final ScheduledExecutorService async;
    private final Possible<ScheduledFuture<?>> asyncJob;
    private LocalDateTime time;

    public AgentOfTable(
            MessagingThroughJdbcImpl jdbcMessaging,
            String agentName,
            Messaging.Destination.Type type,
            String destinationName) {
        super(agentName);
        this.jdbcMessaging = jdbcMessaging;
        this.jdbc = this.jdbcMessaging.jdbc;
        this.async = this.jdbcMessaging.namedThreadSource.newNamedScheduledExecutorService(
                type.typeName + "[" + destinationName +"].Agent[" + agentName + "]",
                1);
        this.asyncJob = References.simplePossibleButEmpty();
        this.createAgentIfNotExists();
    }

    private void createAgentIfNotExists() {
        this.jdbc.doInTransaction(transaction -> {
            LocalDateTime agentCreationTime;

            Optional<LocalDateTime> storedTime = transaction.doQueryAndConvertFirstRow(
                    row -> row.timeOf("time"),
                    "SELECT * \n" +
                    "FROM jdbc_m_agents \n" +
                    "WHERE name = ?",
                    super.name());

            if ( storedTime.isEmpty() ) {
                agentCreationTime = now();
                transaction.doUpdate(
                        "INSERT INTO jdbc_m_agents (name, time) \n" +
                        "VALUES (?, ?)",
                        super.name(), agentCreationTime);
            }
            else {
                agentCreationTime = storedTime.get();
            }

            this.time = agentCreationTime;
        });
    }

    protected abstract void doScheduledLogic();

    private void doScheduledLogicSafely() {
        try {
            this.doScheduledLogic();
        }
        catch (Throwable t) {
            log.info(t.getMessage(), t);
            if ( t instanceof Error ) {
                throw t;
            }
        }
    }

    @Override
    protected boolean doSynchronizedStartWork() {
        this.asyncJob.resetTo(this.async.scheduleWithFixedDelay(this::doScheduledLogicSafely, 200, 200, MILLISECONDS));
        return true;
    }

    @Override
    protected boolean doSynchronizedPauseWork() {
        this.asyncJob.extractOrThrow().cancel(true);
        return true;
    }

    @Override
    protected boolean doSynchronizedDestroy() {
        ThreadsUtil.shutdownAndWait(this.async);
        return true;
    }

    @Override
    public LocalDateTime createdAt() {
        requireNonNull(this.time);
        return this.time;
    }
}
