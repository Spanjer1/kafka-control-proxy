# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-slim

# Set the working directory in the container
WORKDIR /app

# Copy the fat jar to the container
COPY ./target/transform-add-header-999-SNAPSHOT-fat.jar /app/application.jar
COPY ./src/main/resources/META-INF /app/resources/

# Run the jar file
ENTRYPOINT ["java", "-jar", "/app/application.jar"]
