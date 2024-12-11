package com.github.liyue2008.actor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

/**
 * Actor model
 * Actor 维护自己的私有状态，可以在认为安全的情况下，通过只读接口暴露部分状态给外部调用。
 * Actor 可以发送消息，接收其他Actor的消息。
 * Actor 消息处理都是单线程的。
 * 要尽量避免在处理消息的方法中有长时间的IO操作。
 */
@SuppressWarnings("UnusedReturnValue")
public class Actor {

    private final static int DEFAULT_INBOX_CAPACITY = 65536;
    private final static int DEFAULT_OUTBOX_CAPACITY = 65536;
    // 地址
    private final String addr;
    // 收件箱，所有收到的消息放入收件箱暂存，然后单线程顺序处理。
    private final ActorInbox inbox;
    // 发件箱，所有发出去的消息放入发件箱暂存，然后邮递员会将消息分发给对应地址
    private final ActorOutbox outbox;
    // 对请求/响应模式的封装支持
    private final ActorResponseSupport responseSupport;

    private final boolean enableMetric;

    /**
     * 判断Actor是否使用私有线程
     * @return 如果使用私有线程返回true，否则返回false
     */
    public boolean isPrivateThread() {
        return privateThread;
    }

    // 是否独占线程，独占线程有更好的性能
    private final boolean privateThread;

    private Actor(String addr, int inboxCapacity, int outboxCapacity, Map<String, Integer> topicQueueMap, boolean privateThread, boolean enableMetric) {
        this.addr = addr;
        this.outbox = new ActorOutbox(outboxCapacity, addr, topicQueueMap, enableMetric);
        this.inbox = new ActorInbox(inboxCapacity, addr, topicQueueMap, outbox);
        this.responseSupport = new ActorResponseSupport(inbox, outbox);
        this.privateThread = privateThread;
        this.enableMetric = enableMetric;
    }

    /**
     * 获取指定主题的收件箱队列大小
     * @param topic 消息主题
     * @return 队列中的消息数量
     */
    public int getInboxQueueSize(String topic) {
        return inbox.getQueueSize(topic);
    }

    /**
     * 获取Actor地址
     * @return 地址
     */
    public String getAddr() {
        return addr;
    }
    /**
     * 添加没有参数的消息处理函数
     * @param topic 消息主题
     * @param handler 消息处理函数
     */
    private void addActorListener(String topic, Runnable handler) {
        inbox.addActorListener(topic, handler);
    }
    /**
     * 添加没有参数的消息处理函数，函数返回值将作为响应消息发送给发送者
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <R> 返回值类型
     */
    private <R> void addActorListener(String topic, Supplier<R> handler) {
        inbox.addActorListener(topic, handler);
    }
    /**
     * 添加1个参数的消息处理函数，函数返回值将作为响应消息发送给发送者
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <T> 参数的类型
     * @param <R> 返回值类型
     */
    private <T, R> void addActorListener(String topic, Function<T, R> handler) {
        inbox.addActorListener(topic, handler);
    }
    /**
     * 添加有2个参数的消息理函数，函数返回值将作为响应消息发送给发送者
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <T> 第一个参数类型
     * @param <U> 第二个参数类型
     * @param <R> 返回值类型
     */
    private <T, U, R> void addActorListener(String topic, BiFunction<T, U, R> handler) {
        inbox.addActorListener(topic, handler);
    }

    /**
     * 添加有2个参数的消息处理函数
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <T> 第一个参数类型
     * @param <U> 第二个参数类型
     */
    private <T, U> void addActorListener(String topic, BiConsumer<T, U> handler) {
        inbox.addActorListener(topic, handler);
    }

    /**
     * 添加有1个参数的消息处理函数
     * @param topic 消息主题
     * @param handler 消息处理函数
     * @param <T> 参数类型。
     */
    private <T> void addActorListener(String topic, Consumer<T> handler) {
        inbox.addActorListener(topic, handler);
    }

    private void addActorSubscriber(String topic, Runnable runnable) {
        inbox.addActorSubscriber(topic, runnable);
    }
    private <T> void addActorSubscriber(String topic, Consumer<T> consumer) {
        inbox.addActorSubscriber(topic, consumer);
    }

    private <T, U> void addActorSubscriber(String topic, BiConsumer<T, U> consumer) {
        inbox.addActorSubscriber(topic, consumer);
    }

    /**
     * 添加定时任务
     * @param interval 时间间隔
     * @param timeUnit 时间单位
     * @param runnable 要执行的任务
     */
    public void addActorScheduler(long interval, TimeUnit timeUnit,Runnable runnable) {
        String topic = runnable.getClass().getName() + "#run()";
        addActorScheduler( new ScheduleTask(timeUnit, interval, this.addr, topic), runnable);
    }

    private void addActorScheduler(ScheduleTask scheduleTask, Runnable runnable) {
        inbox.receive(new ActorMsg(0L, addr,addr,"@addActorListener", scheduleTask.getTopic(), runnable));
        send("Scheduler", "addTask", scheduleTask);
    }

    private void addActorScheduler(SchedulerRequest request) {
        addActorScheduler(request.getInterval(), request.getTimeUnit(), request.getRunnable());
    }

    /**
     * 延迟执行任务
     * @param delay 延迟时间
     * @param timeUnit 时间单位
     * @param runnable 要执行的任务
     */
    public void runDelay(long delay, TimeUnit timeUnit,  Runnable runnable) {
        String topic = runnable.getClass().getName() + "#run()";
        inbox.receive(new ActorMsg(0L, addr,addr,"@addActorListener", topic, runnable));
        send("Scheduler", "addDelayTask", new DelayTask(timeUnit, delay, this.addr, topic));
    }

    /**
     * 移除定时任务
     * @param runnable 要移除的任务
     */
    public void removeScheduler(Runnable runnable) {
        String topic = runnable.getClass().getName() + "#run()";
        inbox.receive(new ActorMsg(0L, addr,addr,"@removeActorListener", topic, runnable));
        send("Scheduler", "removeTask", addr, topic);
    }
    /**
     * 设置默认消息处理函数。所有未处理的消息都由这个函数处理。
     * @param handler 默认的消息处理函数
     */
    private void setDefaultHandlerFunction(Consumer<ActorMsg> handler) {
        inbox.setDefaultHandlerFunction(handler);
    }

    private void setHandlerInstance(Object handlerInstance) {
        inbox.setHandlerInstance(handlerInstance);
    }

    /**
     * 向指定主题发布消息
     * @param topic 消息主题
     * @param payload 消息内容
     * @return 发送的消息对象
     */
    public ActorMsg pub(String topic, Object payload) {
        return send(PubSubActor.ADDR, topic, payload);
    }

    /**
     * 向指定主题发布无内容消息
     * @param topic 消息主题
     * @return 发送的消息对象
     */
    public ActorMsg pub(String topic) {
        return send(PubSubActor.ADDR, topic);
    }

    /**
     * 向指定主题发布多个消息内容
     * @param topic 消息主题
     * @param payloads 多个消息内容
     * @return 发送的消息对象
     */
    public ActorMsg pub(String topic, Object... payloads) {
        return send(PubSubActor.ADDR, topic, payloads);
    }

    /**
     * 异步发布消息并返回CompletableFuture
     * @param topic 消息主题
     * @return 异步操作的CompletableFuture
     */
    public CompletableFuture<Void> pubThen(String topic) {
        return this.<CompletableFuture<Void>>sendThen(PubSubActor.ADDR, topic).thenCompose(f -> f);
    }

    /**
     * 异步发布多个消息内容并返回CompletableFuture
     * @param topic 消息主题
     * @param payloads 多个消息内容
     * @return 异步操作的CompletableFuture
     */
    public CompletableFuture<Void> pubThen(String topic, Object... payloads) {
        return this.<CompletableFuture<Void>>sendThen(PubSubActor.ADDR, topic, payloads).thenCompose(f -> f);
    }

    /**
     * 发送消息到指定地址和主题
     * @param addr 目标地址
     * @param topic 消息主题
     * @return 发送的消息对象
     */
    public ActorMsg send(String addr, String topic) {
        return send(addr, topic, ActorMsg.Response.DEFAULT);
    }

    /**
     * 发送消息到指定地址和主题，并指定响应类型
     * @param addr 目标地址
     * @param topic 消息主题
     * @param response 响应类型
     * @return 发送的消息对象
     */
    public ActorMsg send(String addr, String topic, ActorMsg.Response response) {
        return send(addr, topic, response, new Object[]{});
    }

    /**
     * 发送消息和内容到指定地址和主题
     * @param addr 目标地址
     * @param topic 消息主题
     * @param payloads 消息内容
     * @return 发送的消息对象
     */
    public ActorMsg send(String addr, String topic, Object... payloads) {
        return send(addr, topic,ActorMsg.Response.DEFAULT, payloads);
    }

    /**
     * 发送消息到指定地址和主题，并指定响应类型和消息内容
     * @param addr 目标地址
     * @param topic 消息主题
     * @param response 响应类型
     * @param payloads 消息内容
     * @return 发送的消息对象
     */
    public ActorMsg send(String addr, String topic, ActorMsg.Response response, Object... payloads) {
        return outbox.send(addr, topic, response, payloads);
    }

    /**
     * 发送消息到指定地址和主题，并指定响应类型和拒绝策略
     * @param addr 目标地址
     * @param topic 消息主题
     * @param response 响应类型
     * @param rejectPolicy 拒绝策略
     * @param payloads 消息内容
     * @return 发送的消息对象
     */
    public ActorMsg send(String addr, String topic, ActorMsg.Response response, ActorRejectPolicy rejectPolicy, Object... payloads) {
        return outbox.send(addr, topic, response, rejectPolicy, payloads);
    }

    /**
     * 异步发送消息并返回CompletableFuture
     * @param addr 目标地址
     * @param topic 消息主题
     * @param <T> 返回值类型
     * @return 异步操作的CompletableFuture
     */
    public <T> CompletableFuture<T> sendThen(String addr, String topic) {
        return sendThen(addr, topic, ActorRejectPolicy.EXCEPTION);
    }

    /**
     * 异步发送消息并指定拒绝策略
     * @param addr 目标地址
     * @param topic 消息主题
     * @param rejectPolicy 拒绝策略
     * @param <T> 返回值类型
     * @return 异步操作的CompletableFuture
     */
    public <T> CompletableFuture<T> sendThen(String addr, String topic, ActorRejectPolicy rejectPolicy) {
        return sendThen(addr, topic, rejectPolicy, new Object[]{});
    }

    /**
     * 异步发送消息和内容
     * @param addr 目标地址
     * @param topic 消息主题
     * @param payloads 消息内容
     * @param <T> 返回值类型
     * @return 异步操作的CompletableFuture
     */
    public <T> CompletableFuture<T> sendThen(String addr, String topic, Object... payloads) {
        return sendThen(addr, topic, ActorRejectPolicy.EXCEPTION,payloads);
    }

    /**
     * 异步发送消息���并指定拒绝策略和消息内容
     * @param addr 目标地址
     * @param topic 消息主题
     * @param rejectPolicy 拒绝策略
     * @param payloads 消息内容
     * @param <T> 返回值类型
     * @return 异步操作的CompletableFuture
     */
    public <T> CompletableFuture<T> sendThen(String addr, String topic, ActorRejectPolicy rejectPolicy, Object... payloads) {
        return responseSupport.send(addr, topic, rejectPolicy, payloads);
    }

    private void addTopicResponseHandlerFunction(String topic, Consumer<ActorMsg> handler) {
        responseSupport.addTopicHandlerFunction(topic, handler);
    }

    private void setResponseHandlerInstance(Object handlerInstance) {
        responseSupport.setHandlerInstance(handlerInstance);
    }

    /**
     * 回复请求消息
     * @param request 原始请求消息
     * @param result 响应结果
     */
    public void reply(ActorMsg request, Object result) {
        responseSupport.reply(request, result);
    }

    /**
     * 回复异常信息
     * @param request 原始请求消息
     * @param throwable 异常信息
     */
    public void replyException(ActorMsg request, Throwable throwable) {
        responseSupport.replyException(request, throwable);
    }

    private void setDefaultResponseHandlerFunction(Consumer<ActorMsg> handler) {
        responseSupport.setDefaultHandlerFunction(handler);

    }

    ActorInbox getInbox() {
        return inbox;
    }

    ActorOutbox getOutbox() {
        return outbox;
    }

    /**
     * 获取Actor构建器
     * @return Actor构建器实例
     */
    public static Builder builder() {
        return new Builder();
    }

    boolean outboxCleared() {
        return outbox.cleared();
    }

    boolean inboxCleared() {
        return inbox.cleared();
    }

    /**
     * 判断是否启用了指标收集
     * @return 如果启用了指标收集返回true，否则返回false
     */
    public boolean isEnableMetric() {
        return enableMetric;
    }

    // Builder
    public static class Builder {
        private String addr;
        private final Map<String, Supplier<?>> topicHandlerSupplierMap = new HashMap<>(); // <topic, supplier>
        private final Map<String, Function<?, ?>> topicHandlerFunctionMap = new HashMap<>(); // <topic, handler>
        private final Map<String, BiFunction<?, ?, ?>> topicHandlerBiFunctionMap = new HashMap<>(); // <topic, handler>
        private final Map<String, BiConsumer<?, ?>> topicHandlerBiConsumerMap = new HashMap<>(); // <topic, handler>
        private final Map<String, Runnable> topicHandlerRunnableMap = new HashMap<>(); // <topic, handler>
        private final Map<String, Consumer<?>> topicHandlerConsumerMap = new HashMap<>(); // <topic, handler>
        private final Map<String, Consumer<ActorMsg>> topicHandlerResponseConsumerMap = new HashMap<>();
        private final Map<String, Runnable> subscriberHandlerRunnableMap = new HashMap<>(); // <topic, handler>
        private final Map<String, Consumer<?>> subscriberHandlerConsumerMap = new HashMap<>(); // <topic, handler>
        private final Map<String, BiConsumer<? , ?>> subscriberHandlerBiConsumerMap = new HashMap<>();
        private final List<SchedulerRequest> schedulerRequestList = new ArrayList<>();
        private Consumer<ActorMsg> defaultHandlerFunction = null; // <topic, handler>
        private Object handlerInstance = null;
        private Object responseHandlerInstance = null;
        private int inboxCapacity = DEFAULT_INBOX_CAPACITY;
        private int outBoxCapacity = DEFAULT_OUTBOX_CAPACITY;
        private final Map<String, Integer> topicQueueMap = new HashMap<>();
        private Consumer<ActorMsg> defaultResponseHandlerFunction = null;
        private boolean privateThread = false;
        private boolean enableMetric = false;
        private Builder() {}



        public <R> Builder addActorListener(String topic, Supplier<R> handler) {
            this.topicHandlerSupplierMap.put(topic, handler);
            return this;
        }

        public <T, R> Builder addActorListener(String topic, Function<T, R> handler) {
            this.topicHandlerFunctionMap.put(topic, handler);
            return this;
        }

        public <T, U, R> Builder addActorListener(String topic, BiFunction<T, U, R> handler) {
            this.topicHandlerBiFunctionMap.put(topic, handler);
            return this;
        }
        public Builder addActorListener(String topic, Runnable handler) {
            this.topicHandlerRunnableMap.put(topic, handler);
            return this;
        }
        public <T, U> Builder addActorListener(String topic, BiConsumer<T, U> handler) {
            this.topicHandlerBiConsumerMap.put(topic, handler);
            return this;
        }

        public <T> Builder addActorSubscriber(String topic, Consumer<T> handler) {
            this.subscriberHandlerConsumerMap.put(topic, handler);
            return this;
        }


        public Builder addActorSubscriber(String topic, Runnable handler) {
            this.subscriberHandlerRunnableMap.put(topic, handler);
            return this;
        }
        public <T, U> Builder addActorSubscriber(String topic, BiConsumer<T, U> handler) {
            this.subscriberHandlerBiConsumerMap.put(topic, handler);
            return this;
        }

        public <T> Builder addActorListener(String topic, Consumer<T> handler) {
            this.topicHandlerConsumerMap.put(topic, handler);
            return this;
        }
        public Builder setDefaultHandlerFunction(Consumer<ActorMsg> handler) {
            this.defaultHandlerFunction = handler;
            return this;
        }

        public Builder setHandlerInstance(Object handlerInstance) {
            this.handlerInstance = handlerInstance;
            return this;
        }

        public Builder addResponseHandlerFunction(String topic, Consumer<ActorMsg> handler) {
            this.topicHandlerResponseConsumerMap.put(topic, handler);
            return this;
        }

        public Builder setResponseHandlerInstance(Object handlerInstance) {
            this.responseHandlerInstance = handlerInstance;
            return this;
        }

        public Builder setDefaultResponseHandlerFunction(Consumer<ActorMsg> handler) {
            this.defaultResponseHandlerFunction = handler;
            return this;
        }

        public Builder privateThread(boolean privateThread) {
            this.privateThread = privateThread;
            return this;
        }
        public Builder inboxCapacity(int inboxCapacity) {
            this.inboxCapacity = inboxCapacity;
            return this;
        }
        public Builder outBoxCapacity(int outBoxCapacity) {
            this.outBoxCapacity = outBoxCapacity;
            return this;
        }

        public Builder addTopicQueue(String topic) {
            this.topicQueueMap.put(topic, -1);
            return this;
        }
        public Builder addTopicQueue(String topic, int queueCapacity) {
            this.topicQueueMap.put(topic, queueCapacity);
            return this;
        }

        public Builder addr(String addr) {
            this.addr = addr;
            return this;
        }

        public Builder enableMetric() {
            this.enableMetric = true;
            return this;
        }

        public Builder addScheduler(long interval, TimeUnit timeUnit, Runnable runnable) {
            this.schedulerRequestList.add(
                    new SchedulerRequest(interval, timeUnit, runnable)
            );
            return this;
        }
        public Actor build() {
            Actor actor = new Actor(addr, inboxCapacity, outBoxCapacity, topicQueueMap, privateThread, enableMetric);
            this.topicHandlerRunnableMap.forEach(actor::addActorListener);
            this.topicHandlerSupplierMap.forEach(actor::addActorListener);
            this.topicHandlerFunctionMap.forEach(actor::addActorListener);
            this.topicHandlerBiFunctionMap.forEach(actor::addActorListener);
            this.topicHandlerBiConsumerMap.forEach(actor::addActorListener);
            this.topicHandlerConsumerMap.forEach(actor::addActorListener);
            this.subscriberHandlerRunnableMap.forEach(actor::addActorSubscriber);
            this.subscriberHandlerConsumerMap.forEach(actor::addActorSubscriber);
            this.subscriberHandlerBiConsumerMap.forEach(actor::addActorSubscriber);
            this.schedulerRequestList.forEach(actor::addActorScheduler);
            if (this.defaultHandlerFunction != null) {
                actor.setDefaultHandlerFunction(this.defaultHandlerFunction);
            }
            if (this.handlerInstance != null) {
                actor.setHandlerInstance(this.handlerInstance);
            }
            this.topicHandlerResponseConsumerMap.forEach(actor::addTopicResponseHandlerFunction);
            if (this.responseHandlerInstance != null) {
                actor.setResponseHandlerInstance(this.responseHandlerInstance);
            }
            if (this.defaultResponseHandlerFunction != null) {
                actor.setDefaultResponseHandlerFunction(this.defaultResponseHandlerFunction);
            }

            return actor;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Actor actor = (Actor) o;
        return Objects.equals(addr, actor.addr);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(addr);
    }

    private static class SchedulerRequest {
        private final long interval;
        private final TimeUnit timeUnit;
        private final Runnable runnable;

        public SchedulerRequest(long interval, TimeUnit timeUnit, Runnable runnable) {
            this.interval = interval;
            this.timeUnit = timeUnit;
            this.runnable = runnable;
        }

        public long getInterval() {
            return interval;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        public Runnable getRunnable() {
            return runnable;
        }
    }

}


