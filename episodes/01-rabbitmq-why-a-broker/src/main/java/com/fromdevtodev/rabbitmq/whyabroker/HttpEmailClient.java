package com.fromdevtodev.rabbitmq.whyabroker;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Calls the (fake) email provider over HTTP and waits for its answer.
 *
 * <p>If the timeout fires you get an {@link IOException}, but you do <em>not</em> know whether the provider
 * sent the email: a timeout is an unknown outcome, not a failure.
 */
public final class HttpEmailClient implements EmailClient {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final URI baseUri;
    private final Duration timeout;

    public HttpEmailClient(URI baseUri, Duration timeout) {
        this.baseUri = baseUri;
        this.timeout = timeout;
    }

    @Override
    public void sendConfirmation(Order order) throws IOException {
        String query = "orderId=" + URLEncoder.encode(order.id(), StandardCharsets.UTF_8)
                + "&customerId=" + URLEncoder.encode(order.customerId(), StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder(baseUri.resolve("/send?" + query))
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<Void> response = http.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() / 100 != 2) {
                throw new IOException("email provider answered HTTP " + response.statusCode());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for the email provider", e);
        }
    }
}
