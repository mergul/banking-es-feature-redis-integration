#!/bin/bash

# Wrapper script for connect-standalone.sh that sets proper logging configuration
# This script sets the KAFKA_LOGS_DIR environment variable and uses a custom log4j config

# Set the logs directory
export KAFKA_LOGS_DIR=/home/kafka/kafka/logs

# Create a temporary log4j configuration that uses the environment variable
TEMP_LOG4J_CONFIG=$(mktemp)
cat > "$TEMP_LOG4J_CONFIG" << EOF
# Temporary log4j configuration that uses KAFKA_LOGS_DIR environment variable
log4j.rootLogger=INFO, stdout, connectAppender

# Send the logs to the console.
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout

# Send the logs to a file, rolling the file at midnight local time.
log4j.appender.connectAppender=org.apache.log4j.DailyRollingFileAppender
log4j.appender.connectAppender.DatePattern='.'yyyy-MM-dd-HH
log4j.appender.connectAppender.File=$KAFKA_LOGS_DIR/connect.log

log4j.appender.connectAppender.layout=org.apache.log4j.PatternLayout

# The `%X{connector.context}` parameter in the layout includes connector-specific and task-specific information
# in the log messages, where appropriate. This makes it easier to identify those log messages that apply to a
# specific connector.
connect.log.pattern=[%d] %p %X{connector.context}%m (%c:%L)%n

log4j.appender.stdout.layout.ConversionPattern=\${connect.log.pattern}
log4j.appender.connectAppender.layout.ConversionPattern=\${connect.log.pattern}

log4j.logger.org.reflections=ERROR
EOF

# Set KAFKA_OPTS to use our custom log4j configuration
export KAFKA_OPTS="-Dlog4j.configuration=file:$TEMP_LOG4J_CONFIG"

# Call the original connect-standalone.sh script
exec /home/kafka/kafka/bin/connect-standalone.sh "$@"

# Clean up (this won't execute due to exec, but good practice)
rm -f "$TEMP_LOG4J_CONFIG"
