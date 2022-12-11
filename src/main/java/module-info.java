module diarsid.messaging.jdbc {

    requires org.slf4j;
    requires diarsid.support;
    requires diarsid.messaging.api;
    requires diarsid.jdbc;

    exports diarsid.messaging.jdbc.api;
    exports diarsid.messaging.jdbc.defaultimpl.cleaning;
    exports diarsid.messaging.jdbc.defaultimpl.serialization;
}
