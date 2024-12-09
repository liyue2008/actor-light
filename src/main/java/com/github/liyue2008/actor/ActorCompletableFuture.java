package com.github.liyue2008.actor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ActorCompletableFuture<T> extends CompletableFuture<T> {
    private final static ThreadLocal<ActorThreadContext> contextThreadLocal = new ThreadLocal<>();

    @Override
    public T get() throws InterruptedException, ExecutionException {
        checkThread();
        return super.get();
    }

    private void checkThread() {
        ActorThreadContext context = contextThreadLocal.get();
        if (null != context && context.isActorThread()) {
            throw new IllegalAccessError("Can not get result from a actor thread!");
        }
    }

    @Override
    public T join() {
        checkThread();
        return super.join();
    }
}
