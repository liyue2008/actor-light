# Actor Light

Actor Light is a lightweight actor framework for Java.

## Features

- Lightweight: No dependencies, just a single jar file.
- Easy to use: Just add the jar file to your project and start using it.
- High performance: Actor Light is designed to be highly performant and scalable.
- Well tested: Actor Light is well tested and has a comprehensive test suite.

## Getting Started

To get started with Actor Light, simply add the following dependency to your project:

```xml
<dependency>
    <groupId>com.github.liyue2008</groupId>
    <artifactId>actor-light</artifactId>
    <version>1.0.0</version>
</dependency>
```

A Hello World example:

```java
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
```

More examples can be found in the [examples](src/main/java/com/github/liyue2008/actor/example) directory.
