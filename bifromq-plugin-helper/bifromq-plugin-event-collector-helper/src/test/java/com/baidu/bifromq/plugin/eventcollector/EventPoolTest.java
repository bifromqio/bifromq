package com.baidu.bifromq.plugin.eventcollector;

import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;

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
