# Intersite "offline" communication

The goal is to provide nats synchronization of streams between different nats server without a direct tcp connection. This can happen through files or http, but there is an intermediary that may have arbitrary latency and packet loss.

Then Request-Reply patterns etc. can be built upon this foundation.
