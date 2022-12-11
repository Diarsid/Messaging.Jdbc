package diarsid.messaging.jdbc.defaultimpl.cleaning;

import java.util.concurrent.TimeUnit;

import diarsid.messaging.defaultimpl.CleaningAtTime;

public abstract class CleanTable extends CleaningAtTime {

    public CleanTable(int interval, TimeUnit unit) {
        super(interval, unit);
    }
}
