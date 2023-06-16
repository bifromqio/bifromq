package com.baidu.bifromq.plugin.eventcollector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class EventPoolTest {
    @Test
    public void get() {
        EventPool pool = new EventPool();
        for (EventType type : EventType.values()) {
            Event<?> event = pool.get(type);
            assertNotNull(event);
            assertEquals(type, event.type());
        }
    }
}
