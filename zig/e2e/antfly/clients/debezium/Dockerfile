# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
FROM maven:3.9.9-eclipse-temurin-21@sha256:3a4ab3276a087bf276f79cae96b1af04f53731bec53fb2e651aca79e4b10211e AS build
WORKDIR /client
COPY pom.xml .
COPY src ./src
RUN mvn --batch-mode --no-transfer-progress -Dmaven.repo.local=/client/.m2 package
FROM eclipse-temurin:21-jre@sha256:ca73b4d3961703d6d7830a4361f49ef0478f8f28b39defc07bb72f4cc7f834f6
COPY --from=build /client/target/debezium-client-1.0.0.jar /client.jar
ENTRYPOINT ["java", "-Dorg.slf4j.simpleLogger.logFile=System.err", "-jar", "/client.jar"]
