package com.github.liyue2008.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class ActorSystem{

    private static final Logger logger = LoggerFactory.getLogger(ActorSystem.class);
    private final Map<String, ActorInbox> inboxMap = new HashMap<>();
    private final List<ActorThread> actorThreadList;
    private final static int DEFAULT_ACTOR_THREAD_COUNT = 1;
    private final ScheduleActor scheduleActor;
    private final List<Actor> actorList;
    private final String name;
    private final PubSubActor pubSubActor = new PubSubActor();
    private final Thread shutdownThread;

    private ActorSystem(int threadCount, List<Actor> actorList, String name) {
        this.name = null == name ? "" : name;
        this.scheduleActor = new ScheduleActor(this.name);
        this.actorList = new ArrayList<>(actorList.size() + 2);
        this.actorList.add(pubSubActor.getActor());
        this.actorList.add(scheduleActor.getActor());
        this.actorList.addAll(actorList);
        this.actorList.forEach(this::addActor);


        List<List<ActorInbox>> threadInboxList = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threadInboxList.add(new ArrayList<>());
        }
        List<List<ActorOutbox>> threadOutboxList = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threadOutboxList.add(new ArrayList<>());
        }
        int threadIndex = 0;

        this.actorThreadList = new ArrayList<>(threadCount);

        for (Actor actor: this.actorList) {
            if (actor.isPrivateThread()) {
                ActorThread actorThread = ActorThread.builder().actorSystem(this)
                        .name("ActorThread-" + (this.name.isEmpty() ? "" : (this.name + "-")) + actor.getAddr())
                        .addInbox(actor.getInbox())
                        .addOutbox(actor.getOutbox()).build();
                this.actorThreadList.add(actorThread);
            } else {
                threadInboxList.get(threadIndex++ % threadCount).add(actor.getInbox());
                threadOutboxList.get(threadIndex++ % threadCount).add(actor.getOutbox());
            }
        }

        for (int i = 0; i < threadCount; i++) {
            ActorThread.Builder builder = ActorThread.builder().actorSystem(this)
                    .name("ActorThread-" + (this.name.isEmpty() ? "" : (this.name + "-")) + i);
            threadInboxList.get(i).forEach(builder::addInbox);
            threadOutboxList.get(i).forEach(builder::addOutbox);
            this.actorThreadList.add(builder.build());
        }
        start();
        shutdownThread = new Thread(this::doStop);
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(shutdownThread);

    }

    private String name() {
        return "Actor system " + this.name;
    }

    private void addActor(Actor actor) {
        inboxMap.put(actor.getInbox().getMyAddr(), actor.getInbox());
        actor.getInbox().getSubscribedTopics().forEach(topic -> pubSubActor.subTopic(topic, actor));
        actor.getInbox().getSchedulers().forEach(scheduleActor::addTask);
    }


    void send(ActorMsg msg) {
        ActorInbox inbox = inboxMap.get(msg.getReceiver());
        if (inbox == null) {
            logger.warn("Receiver not fond! msg: {}", msg);
            return;
        }
        inbox.receive(msg);
    }

    private void start() {
        actorThreadList.forEach(ActorThread::start);
    }

    private boolean hasMessages() {
        return !actorList.stream().allMatch(actor -> actor.outboxCleared()  && actor.inboxCleared());
    }

    public void stop() {
        Runtime.getRuntime().removeShutdownHook(shutdownThread);
        doStop();
    }

    private void doStop() {
        try {
            // 停止接收新的定时任务
            // 取消所有定时任务
            scheduleActor.stop();


            // 停止所有ActorThread线程
            for (ActorThread actorThread : actorThreadList) {
                actorThread.stop();
            }

            // 处理所有剩余的消息，直到全部消息都处理完成。
            while (hasMessages()) {
                actorList.stream().map(Actor::getOutbox).forEach(outbox -> {
                   boolean hasMessages = true;
                   while (hasMessages) {
                       hasMessages = outbox.consumeOneMsg(this::send);
                   }
                });
                actorList.stream().map(Actor::getInbox).forEach(inbox -> {
                    boolean hasMessages = true;
                    while (hasMessages) {
                        hasMessages = inbox.processOneMsg();
                    }
                });
            }
            logger.info("{} stopped.", name());
        } catch (InterruptedException e) {
            logger.warn("Stop actor system exception!", e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
    public static class Builder {
        private Builder() {}
        private int threadCount = DEFAULT_ACTOR_THREAD_COUNT;
        private final List<Actor> actorList = new ArrayList<>();
        private String name = null;

        public Builder threadCount(int threadCount) {
            this.threadCount = threadCount;
            return this;
        }

        public Builder addActor(Actor actor) {
            this.actorList.add(actor);
            return this;
        }

        public ActorSystem build() {
            return new ActorSystem(threadCount, actorList, name);
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }
    }
}
