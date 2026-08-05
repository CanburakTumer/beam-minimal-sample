# Stage 1: Build shaded JAR using Gradle
FROM gradle:8.10-jdk21 AS build
WORKDIR /home/gradle/src
COPY --chown=gradle:gradle . .
RUN gradle shadowJar --no-daemon

# Stage 2: Dataflow Flex Template Launcher Image
FROM gcr.io/dataflow-templates-base/java21-template-launcher-base:latest

ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS="com.example.beam.LoremIpsumWordCountPipeline"
ENV FLEX_TEMPLATE_JAVA_CLASSPATH="/template/beam-minimal-sample.jar"

COPY --from=build /home/gradle/src/build/libs/beam-minimal-sample.jar /template/beam-minimal-sample.jar
