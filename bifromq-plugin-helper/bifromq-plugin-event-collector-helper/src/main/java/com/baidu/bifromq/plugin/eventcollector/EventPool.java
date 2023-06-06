/*
 * Copyright (c) 2023. Baidu, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.baidu.bifromq.plugin.eventcollector;

import static org.reflections.scanners.Scanners.SubTypes;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.SneakyThrows;
import org.reflections.Reflections;

class EventPool {
    private static final Set<Class<?>> EVENT_TYPES;
    private final Map<Class<? extends Event<?>>, Event<?>> pool = new HashMap<>();

    private final Event<?>[] events;

    static {
        Reflections reflections = new Reflections(Event.class.getPackageName());

        EVENT_TYPES = reflections.get(SubTypes.of(Event.class)
            .asClass()
            .filter(c -> !Modifier.isAbstract(c.getModifiers())));
    }

    EventPool() {
        events = new Event[EVENT_TYPES.size()];
        EVENT_TYPES.forEach(t -> this.add((Class<? extends Event<?>>) t));
    }

    <T extends Event<?>> T get(EventType eventType) {
        return (T) events[eventType.ordinal()];
    }

    @SneakyThrows
    private void add(Class<? extends Event<?>> eventClass) {
        pool.put(eventClass, eventClass.getConstructor().newInstance());
        Event<?> event = eventClass.getConstructor().newInstance();
        events[event.type().ordinal()] = event;
    }
}
