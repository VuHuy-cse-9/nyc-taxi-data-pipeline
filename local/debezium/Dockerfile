FROM alpine:3.20

# Install curl and bash (optional but useful)
RUN apk add --no-cache curl bash

WORKDIR /app

# Copy your script
COPY . .

# Make it executable
RUN chmod +x /app/run.sh
RUN chmod +x /app/run-kafka-connect.sh
RUN chmod +x /app/run-all.sh

# Run script
CMD ["/app/run-all.sh"]
