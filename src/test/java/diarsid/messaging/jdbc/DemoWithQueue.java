package diarsid.messaging.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.h2.jdbcx.JdbcConnectionPool;

import diarsid.jdbc.api.Jdbc;
import diarsid.jdbc.api.JdbcOption;
import diarsid.jdbc.api.SqlConnectionsSource;
import diarsid.messaging.jdbc.defaultimpl.serialization.StringSerialization;
import diarsid.messaging.api.Messaging;

import static diarsid.messaging.jdbc.api.MessagingThroughJdbc.jdbcMessaging;

public class DemoWithQueue {

    private static final Messaging.Receiver<String> RECEIVER = (offset, s) -> {
        System.out.println("[RECEIVED] offset:" + offset + " message:" + s);
    };

    public static class H2Connections implements SqlConnectionsSource {

        private final JdbcConnectionPool connectionsPool;

        H2Connections() {
            this.connectionsPool = JdbcConnectionPool.create("jdbc:h2:file:./src/test/resources/data/sql/h2/messaging;AUTO_SERVER=TRUE", "user", "user");
            this.connectionsPool.setMaxConnections(3);
        }

        @Override
        public Connection getConnection() throws SQLException {
            return this.connectionsPool.getConnection();
        }

        @Override
        public void close() {
            this.connectionsPool.dispose();
        }
    }

    public static class Client1 {

        public static void main(String[] args) {
            SqlConnectionsSource connections = new H2Connections();

            Jdbc jdbc = Jdbc.init(connections, Map.of(JdbcOption.SQL_HISTORY_ENABLED, false));
            Messaging messaging = jdbcMessaging(jdbc);

            Messaging.Queue<String> queue = messaging.queue("queue_1", new StringSerialization());
            Messaging.Agent.OfQueue<String> agentOfQueue = queue.connect("agent-1", RECEIVER);
            agentOfQueue.startWork();
        }
    }

    public static class Client2 {

        public static void main(String[] args) throws Exception {
            SqlConnectionsSource connections = new H2Connections();

            Jdbc jdbc = Jdbc.init(connections, Map.of(JdbcOption.SQL_HISTORY_ENABLED, false));
            Messaging messaging = jdbcMessaging(jdbc);

            Messaging.Queue<String> queue = messaging.queue("queue_1", new StringSerialization());
            Messaging.Agent.OfQueue<String> agentOfQueue = queue.connect("agent-2", RECEIVER);
            agentOfQueue.startWork();

            int counter = 0;
            while (true) {
                Thread.sleep(1035);
                counter++;
                queue.send("message " + counter);
                System.out.println("message sent");
            }
        }
    }

    public static class Client3 {

        public static void main(String[] args) {
            SqlConnectionsSource connections = new H2Connections();

            Jdbc jdbc = Jdbc.init(connections, Map.of(JdbcOption.SQL_HISTORY_ENABLED, false));
            Messaging messaging = jdbcMessaging(jdbc);

            Messaging.Queue<String> queue = messaging.queue("queue_1", new StringSerialization());
            Messaging.Agent.OfQueue<String> agentOfQueue = queue.connect("agent-3", RECEIVER);
            agentOfQueue.startWork();

        }
    }

}
