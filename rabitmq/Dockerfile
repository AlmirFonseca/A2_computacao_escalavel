# Use the official RabbitMQ image from the Docker Hub
FROM rabbitmq:3-management

# Set environment variables for RabbitMQ
ENV RABBITMQ_DEFAULT_USER=guest
ENV RABBITMQ_DEFAULT_PASS=guest

# Expose RabbitMQ ports
# 5672 is for RabbitMQ protocol
# 15672 is for RabbitMQ management UI
EXPOSE 5672 15672

# Add a healthcheck
HEALTHCHECK --interval=10s --retries=10 CMD rabbitmq-diagnostics status
