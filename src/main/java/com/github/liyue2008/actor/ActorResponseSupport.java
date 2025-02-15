package com.github.liyue2008.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.liyue2008.actor.annotation.ActorMessage;
import com.github.liyue2008.actor.annotation.ActorResponseListener;

import static com.github.liyue2008.actor.ActorMsg.RESPONSE;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

class ActorResponseSupport {


    private static final Logger logger = LoggerFactory.getLogger( ActorResponseSupport.class );

    private Consumer<ActorMsg> defaultResponseHandler;

    private final Map<String /* topic */, Consumer<ActorMsg>> responseHandlers;

    private Object handlerInstance;

    private Map<String, List<InvocationTarget>> annotationListeners = new HashMap<>();


    @SuppressWarnings("rawtypes")
    private final Map<ActorMsg, CompletableFuture> responseFutures = new ConcurrentHashMap<>();

    private final ActorOutbox outbox;
    ActorResponseSupport(ActorInbox inbox, ActorOutbox outbox) {
        responseHandlers = new HashMap<>();
        this.outbox = outbox;
        ResponseMessageConsumer responseMessageConsumer = new ResponseMessageConsumer();
        inbox.addActorListener(RESPONSE, responseMessageConsumer, ActorMsg.class);
    }

    <T> CompletableFuture<T> send(String addr, String topic, ActorRejectPolicy rejectPolicy, Object... payloads){
        ActorCompletableFuture<T> future = new ActorCompletableFuture<>();
        ActorMsg request = this.outbox.createMsg(addr, topic, ActorMsg.Response.REQUIRED, rejectPolicy, payloads);
        responseFutures.put(request, future);
        this.outbox.send(request);
        return future;
    }

    void addTopicHandlerFunction(String topic, Consumer<ActorMsg> handler) {
        responseHandlers.put(topic, handler);
    }

    void setHandlerInstance(Object handlerInstance) {
        this.handlerInstance = handlerInstance;
        this.annotationListeners = ActorUtils.scanActionListeners(handlerInstance, ActorResponseListener.class);
    }

    void replyException(ActorMsg request, Throwable throwable) {
        this.outbox.send(this.outbox.createResponse(request, null, throwable));

    }

    void reply(ActorMsg request, Object result) {
        if (request.getContext().getResponseConfig() != ActorMsg.Response.IGNORE) {
            this.outbox.send(this.outbox.createResponse(request, result, null));
        }
    }

    void setDefaultHandlerFunction(Consumer<ActorMsg> handler) {
        this.defaultResponseHandler = handler;
    }



    private void processResponse(ActorMsg response) {
        ActorMsg request = response.getRequest();
        if (request != null) {
            // 调用future
            if (responseFutures.containsKey(request)) {

                CompletableFuture<?> future = responseFutures.remove(request);
                if (response.getThrowable() != null) {
                    future.completeExceptionally(response.getThrowable());
                } else {
                    future.complete(response.getResult());
                }
                return;
            }
            // 显式注册的
            Consumer<ActorMsg> handler = responseHandlers.get(request.getTopic());
            if (handler != null) {
                handler.accept(response);
                return;
            }
            if (handlerInstance != null) {
                try {
                    // 注解注册的
                    if (null != annotationListeners && annotationListeners.containsKey(request.getTopic())){
                        // 通过注解注册的方法

                        List<InvocationTarget> invocationTargets = annotationListeners.get(request.getTopic());
                        if (null != invocationTargets) {
                            for (InvocationTarget target: invocationTargets) {
                                Method method = target.getMethod();
                                method.setAccessible(true);
                                method.invoke(handlerInstance, response);
                            }
                            return;
                        }
                    }
                    // 默认的响应方法
                    Method method = handlerInstance.getClass().getDeclaredMethod(request.getTopic() + "Response", ActorMsg.class);
                    method.setAccessible(true);
                    method.invoke(handlerInstance, response);
                    return;
                } catch (NoSuchMethodException ignored) {
                    // nothing to do
                } catch (IllegalAccessException | InvocationTargetException e) {
                    logger.warn("Invoke response handler failed, method: {}!", request.getTopic() + "Response", e);
                    return;
                }
            }
            if (this.defaultResponseHandler != null) {
                this.defaultResponseHandler.accept(response);

            } else {
                logger.warn("No ongoing request for response: {}", response);
            }
        }
    }

    private class ResponseMessageConsumer implements Consumer<ActorMsg> {
        @Override
        public void accept(@ActorMessage ActorMsg actorMsg) {
            processResponse(actorMsg);
        }
    }
}
