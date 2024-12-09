package com.github.liyue2008.actor;

class ActorThreadContext {
    private final boolean isActorThread;

    public ActorThreadContext(boolean isActorThread) {
        this.isActorThread = isActorThread;
    }

    public boolean isActorThread() {
        return isActorThread;
    }
}
