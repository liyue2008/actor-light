package com.github.liyue2008.actor.example;

import com.github.liyue2008.actor.Actor;
import com.github.liyue2008.actor.ActorSystem;


public class HelloActor {

    public static void main(String[] args) {

        // 创建Actor
        Actor actor = Actor.builder()
            .addr("hello-actor")
            .addActorListener("hello", name -> "Hello, " + name) // 当收到消息的时候返回Hello, name
            .build();
        
        // 创建并启动ActorSystem
        ActorSystem.builder()
            .addActor(actor)
            .build();

        // 发送消息并打印返回的结果
        actor.<String>sendThen("hello-actor", "hello", "world!")
            .thenAccept(greeting -> {
                System.out.println(greeting);
            });
    }

}
