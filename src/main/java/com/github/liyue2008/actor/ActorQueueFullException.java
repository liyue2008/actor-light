package com.github.liyue2008.actor;

public class ActorQueueFullException extends RuntimeException {
    public ActorQueueFullException(String message) {
        super(message);
    }

    public ActorQueueFullException() {
        super();
    }

    public ActorQueueFullException(String message, Throwable t) {
        super(message, t);
    }

    public ActorQueueFullException(Throwable t) {
        super(t);
    }}
