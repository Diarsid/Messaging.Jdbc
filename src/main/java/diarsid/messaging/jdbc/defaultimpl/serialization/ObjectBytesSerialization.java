package diarsid.messaging.jdbc.defaultimpl.serialization;

import diarsid.messaging.api.Messaging;

import static diarsid.messaging.api.Messaging.Destination.Serialization.PersistentType.BYTES;

public class ObjectBytesSerialization implements Messaging.Destination.Serialization<Object> {

    @Override
    public Class<Object> messageType() {
        return Object.class;
    }

    @Override
    public PersistentType persistentType() {
        return BYTES;
    }

    @Override
    public Object deserialize(Object stored) {
        return null;
    }

    @Override
    public Object serialize(Object message) {
        return null;
    }
}
