package com.github.liyue2008.actor;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.github.liyue2008.actor.annotation.ActorMessage;
import com.github.liyue2008.actor.annotation.ResponseManually;

public class PubSubActor {
    public final static String ADDR = "PubSub";
    private final Actor actor;
    private final Map<String, Set<String>> pubSubMap = new ConcurrentHashMap<>();

    public PubSubActor() {
        actor = Actor.builder().addr(ADDR).setDefaultHandlerFunction(this::pubMsg).build();
    }

    void subTopic(String topic, Actor actor) {
        pubSubMap.computeIfAbsent(topic, k -> new HashSet<>()).add(actor.getAddr());
    }

    @ResponseManually
    private void pubMsg(@ActorMessage ActorMsg msg) {
        Set<String> subscribers = pubSubMap.get(msg.getTopic());
        if (subscribers == null) {
            return;
        }
        if (msg.getContext().getResponseConfig() == ActorMsg.Response.REQUIRED) {
            List<CompletableFuture<?>> futures = new ArrayList<>(subscribers.size());
            for (String subscriber : subscribers) {
                CompletableFuture<?> future = actor.sendThen(subscriber, msg.getTopic(), msg.getPayloads());
                futures.add(future);
            }
            actor.reply(msg, CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])));
        } else {
            for (String subscriber : subscribers) {
                actor.send(subscriber, msg.getTopic(), msg.getPayloads());
            }
        }
    }

    Actor getActor() {
        return actor;
    }


}
