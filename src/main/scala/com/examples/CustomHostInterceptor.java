package com.examples;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;

public class CustomHostInterceptor
implements Interceptor {

private String hostValue;
private String hostHeader;

public CustomHostInterceptor(String hostHeader){
this.hostHeader = hostHeader;
}

@Override
public void initialize() {
// At interceptor start up
try {
    hostValue =
            InetAddress.getLocalHost().getHostName();
} catch (UnknownHostException e) {
    throw new FlumeException("Cannot get Hostname", e);
}
}

@Override
public Event intercept(Event event) {

// This is the event's body
String body = new String(event.getBody());

// These are the event's headers
Map<String, String> headers = event.getHeaders();

// Enrich header with hostname
headers.put("key", "123");

// Let the enriched event go
return event;
}

@Override
public List<Event> intercept(List<Event> events) {

List<Event> interceptedEvents =
        new ArrayList<Event>(events.size());
for (Event event : events) {
    // Intercept any event
    Event interceptedEvent = intercept(event);
    interceptedEvents.add(interceptedEvent);
}

return interceptedEvents;
}

@Override
public void close() {
// At interceptor shutdown
}

public static class Builder
    implements Interceptor.Builder {

private String hostHeader;

@Override
public void configure(Context context) {
    // Retrieve property from flume conf
    hostHeader = context.getString("hostHeader");
}

@Override
public Interceptor build() {
    return new CustomHostInterceptor(hostHeader);
}
}
}