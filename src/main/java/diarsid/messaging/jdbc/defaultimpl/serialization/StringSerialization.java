package diarsid.messaging.jdbc.defaultimpl.serialization;

import diarsid.messaging.api.Messaging;

import static diarsid.messaging.api.Messaging.Destination.Serialization.PersistentType.STRING;

public class StringSerialization implements Messaging.Destination.Serialization<String> {

    @Override
    public Class<String> messageType() {
        return String.class;
    }

    @Override
    public PersistentType persistentType() {
        return STRING;
    }

    @Override
    public String deserialize(Object stored) {
        return (String) stored;
    }

    @Override
    public Object serialize(String message) {
        return message;
    }
}
