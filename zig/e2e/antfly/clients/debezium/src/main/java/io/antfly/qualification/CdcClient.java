// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
package io.antfly.qualification;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/** Runs the unmodified PostgreSQL connector; stdout is JSONL, stderr diagnostics. */
public final class CdcClient {
    private CdcClient() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("expected connector properties file path");
        }
        Properties config = new Properties();
        try (InputStream stream = Files.newInputStream(Path.of(args[0]))) {
            config.load(stream);
        }
        // This runner must never substitute an Antfly-specific connector/plugin.
        if (!"io.debezium.connector.postgresql.PostgresConnector".equals(config.getProperty("connector.class"))
                || !"pgoutput".equals(config.getProperty("plugin.name"))) {
            throw new IllegalArgumentException("qualification requires stock PostgresConnector and pgoutput");
        }
        AtomicReference<Throwable> failure = new AtomicReference<>();
        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(config)
                .using((success, message, error) -> {
                    if (!success) failure.set(error != null ? error : new IllegalStateException(message));
                })
                .notifying((records, committer) -> {
                    for (ChangeEvent<String, String> record : records) {
                        System.out.println("{\"topic\":" + quote(record.destination())
                                + ",\"key\":" + jsonOrNull(record.key())
                                + ",\"value\":" + jsonOrNull(record.value()) + "}");
                        System.out.flush();
                        if (System.out.checkError()) throw new IllegalStateException("event output failed");
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                }).build();
        // The async engine closes itself when run exits. A second close can
        // throw and mask the connector's useful completion exception.
        Thread shutdown = new Thread(() -> {
            try { engine.close(); }
            catch (Exception error) { failure.compareAndSet(null, error); }
        }, "qualification-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdown);
        try { engine.run(); }
        finally {
            try { Runtime.getRuntime().removeShutdownHook(shutdown); }
            catch (IllegalStateException ignored) { /* JVM is already shutting down. */ }
        }
        if (failure.get() != null) throw new IllegalStateException("CDC connector failed", failure.get());
    }

    private static String jsonOrNull(String value) { return value == null ? "null" : value; }

    private static String quote(String value) {
        StringBuilder result = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> result.append("\\\"");
                case '\\' -> result.append("\\\\");
                default -> {
                    if (c < 0x20) result.append(String.format("\\u%04x", (int) c));
                    else result.append(c);
                }
            }
        }
        return result.append('"').toString();
    }
}
