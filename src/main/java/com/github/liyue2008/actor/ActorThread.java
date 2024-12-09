package com.github.liyue2008.actor;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ActorThread implements Runnable {

    private final ActorSystem actorSystem;

    private final Object ring = new Object();

    private final List<ActorInbox> inboxList;

    private final List<ActorOutbox> outboxList;


    private final Thread thread;

    private ActorThread(ActorSystem actorSystem, List<ActorInbox> inboxList, List<ActorOutbox> outboxList, String name) {
        this.actorSystem = actorSystem;
        this.inboxList = Collections.unmodifiableList(inboxList);
        this.outboxList = Collections.unmodifiableList(outboxList);
        inboxList.forEach(inbox -> inbox.setRing(ring));
        outboxList.forEach(outbox -> outbox.setRing(ring));
        this.thread = new Thread(this, name);
        thread.setDaemon(true);


    }

    public void start() {
        thread.start();

    }

    private boolean stopFlag = false;

    public void stop() throws InterruptedException {
        stopFlag = true;
        synchronized (ring) {
            ring.notify();
        }
        thread.join();
    }
    @Override
    public void run() {
        ThreadLocal<ActorThreadContext> contextThreadLocal = new ThreadLocal<>();
        contextThreadLocal.set(new ActorThreadContext(true));
        while (!stopFlag) {
            boolean hasMessage = false;

            for (ActorInbox inbox : inboxList) {
                if (inbox.processOneMsg()) {
                    hasMessage = true;
                }
            }
            for (ActorOutbox outbox: outboxList) {
                if (outbox.consumeOneMsg(actorSystem::send)) {
                    hasMessage = true;
                }
            }
            if (!hasMessage) {
                synchronized (ring) {
                    try {
                        ring.wait(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
//        logger.info("{} stopped.", Thread.currentThread().getName());
    }



    static Builder builder() {
        return new Builder();
    }
    static class Builder {
        private String name = "ActorLight";
        private final List<ActorInbox> inboxList = new ArrayList<>();

        private final List<ActorOutbox> outboxList = new ArrayList<>();
        private ActorSystem actorSystem;

        Builder actorSystem(ActorSystem actorSystem) {
            this.actorSystem = actorSystem;
            return this;
        }
        Builder name(String name) {
            this.name = name;
            return this;
        }
        Builder addInbox(ActorInbox inbox) {
            inboxList.add(inbox);
            return this;
        }

        Builder addOutbox(ActorOutbox outbox) {
            outboxList.add(outbox);
            return this;
        }
        ActorThread build() {
            return new ActorThread(actorSystem, inboxList, outboxList, name);
        }
    }
}
