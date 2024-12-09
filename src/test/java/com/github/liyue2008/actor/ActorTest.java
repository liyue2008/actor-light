package com.github.liyue2008.actor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.liyue2008.actor.annotation.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("ALL")
public class ActorTest {
private static final Logger logger = LoggerFactory.getLogger(ActorTest.class);
    @Test
    public void testAddActorListenerNoArg() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        Actor receiver = Actor.builder().addr("receiver")
                .addActorListener("test", latch::countDown)
                .build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
            sender.send("receiver", "test");
            assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testAddActorListenerOneArg() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Actor receiver = Actor.builder().addr("receiver")
                .addActorListener("test", (Consumer<String>) str -> {
                    assertEquals("Hello, world!", str);
                    latch.countDown();
                })
                .build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();

            sender.send("receiver", "test", "Hello, world!");
            assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testAddActorListenerTwoArgs() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Actor receiver = Actor.builder().addr("receiver")
                .addActorListener("test", (str, num) -> {
                    assertEquals("Hello, world!", str);
                    assertEquals(12, num);
                    latch.countDown();
                }).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        
            sender.send("receiver", "test", "Hello, world!", 12);
            assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testAddActorListenerNoArgWithResponse() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addActorListener("test", () -> {
            latch.countDown();
            return "Hello, world!";
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        
            sender.sendThen("receiver", "test").thenAccept(str -> {
                assertEquals("Hello, world!", str);
                latch.countDown();
            });
            assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testAddActorListenerOneArgWithResponse() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addActorListener("test", str -> {
            assertEquals("Hello, world!", str);
            latch.countDown();
            return "Hello, world2!";
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.sendThen("receiver", "test", "Hello, world!").thenAccept(str -> {
                assertEquals("Hello, world2!", str);
                latch.countDown();
            });
            assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testAddActorListenerTwoArgsWithResponse() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addActorListener("test", (str, num) -> {
            assertEquals("Hello, world!", str);
            assertEquals(12, num);
            latch.countDown();
            return "Hello, world2!";
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.sendThen("receiver", "test", "Hello, world!", 12).thenAccept(str -> {
                assertEquals("Hello, world2!", str);
                latch.countDown();
            });
            assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    // TOPIC 同名方法

    @SuppressWarnings({"SameReturnValue", "unused"})
    private static class TopicNameHandlerFunctions {
        private final CountDownLatch latch;

        public TopicNameHandlerFunctions(CountDownLatch latch) {
            this.latch = latch;
        }


        private void noArg() {
            latch.countDown();
        }

        private void oneArg(String str) {
            assertEquals("oneArg", str);
            latch.countDown();
        }

        private void twoArgs(String str, int num) {
            assertEquals("twoArgs", str);
            assertEquals(12, num);
            latch.countDown();
        }

        private void threeArgs(String str, int num, float fnum) {
            assertEquals("threeArgs", str);
            assertEquals(12, num);
            assertEquals(1.2f, fnum, 0.00001);
            latch.countDown();
        }

        private String noArgWithReturn() {
            latch.countDown();
            return "noArgWithReturn";
        }

        private String oneArgWithReturn(String str) {
            assertEquals("oneArg", str);
            latch.countDown();
            return "oneArgWithReturn";
        }

        private String twoArgsWithReturn(String str, int num) {
            assertEquals("twoArgs", str);
            assertEquals(12, num);
            latch.countDown();
            return "twoArgsWithReturn";
        }

        private String threeArgsWithReturn(String str, int num, float fnum) {
            assertEquals("threeArgs", str);
            assertEquals(12, num);
            assertEquals(1.2f, fnum, 0.00001);
            latch.countDown();
            return "threeArgsWithReturn";
        }

    }

    @Test
    public void testAddTopicHandlerFunctionWithTopicName() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(12);
        TopicNameHandlerFunctions handlerFunctions = new TopicNameHandlerFunctions(latch);
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(handlerFunctions).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "noArg");
            sender.sendThen("receiver", "noArgWithReturn").thenAccept(str -> {
                assertEquals("noArgWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "oneArg", "oneArg");
            sender.sendThen("receiver", "oneArgWithReturn", "oneArg").thenAccept(str -> {
                assertEquals("oneArgWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "twoArgs", "twoArgs", 12);
            sender.sendThen("receiver", "twoArgsWithReturn", "twoArgs", 12).thenAccept(str -> {
                assertEquals("twoArgsWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "threeArgs", "threeArgs", 12, 1.2f);
            sender.sendThen("receiver", "threeArgsWithReturn", "threeArgs", 12, 1.2f).thenAccept(str -> {
                assertEquals("threeArgsWithReturn", str);
                latch.countDown();
            });
            assertTrue(latch.await(10, TimeUnit.SECONDS));


    }

    // annotation

    @SuppressWarnings("SameReturnValue")
    private static class AnnotationHandlerFunctions {
        private final CountDownLatch latch;

        public AnnotationHandlerFunctions(CountDownLatch latch) {
            this.latch = latch;
        }

        @ActorListener
        private void noArg() {
            latch.countDown();
        }

        @ActorListener
        private void oneArg(String str) {
            assertEquals("oneArg", str);
            latch.countDown();
        }

        @ActorListener
        private void twoArgs(String str, int num) {
            assertEquals("twoArgs", str);
            assertEquals(12, num);
            latch.countDown();
        }

        @ActorListener
        private void threeArgs(String str, int num, float fnum) {
            assertEquals("threeArgs", str);
            assertEquals(12, num);
            assertEquals(1.2f, fnum, 0.00001);
            latch.countDown();
        }

        @ActorListener
        private String noArgWithReturn() {
            latch.countDown();
            return "noArgWithReturn";
        }

        @ActorListener
        private String oneArgWithReturn(String str) {
            assertEquals("oneArg", str);
            latch.countDown();
            return "oneArgWithReturn";
        }

        @ActorListener
        private String twoArgsWithReturn(String str, int num) {
            assertEquals("twoArgs", str);
            assertEquals(12, num);
            latch.countDown();
            return "twoArgsWithReturn";
        }

        @ActorListener
        private String threeArgsWithReturn(String str, int num, float fnum) {
            assertEquals("threeArgs", str);
            assertEquals(12, num);
            assertEquals(1.2f, fnum, 0.00001);
            latch.countDown();
            return "threeArgsWithReturn";
        }
    }

    // @ActorMessage

    @SuppressWarnings("unused")
    private static class ActorMsgClass {
        private final CountDownLatch latch;

        public ActorMsgClass(CountDownLatch latch) {
            this.latch = latch;
        }

        private void onMessage(@ActorMessage ActorMsg msg) {
            assertEquals("sender", msg.getSender());
            assertEquals("receiver", msg.getReceiver());
            assertEquals("onMessage", msg.getTopic());
            assertEquals("Hello", msg.getPayload());
            latch.countDown();
        }

        @ActorListener
        private void onMessageWithAnnotation(@ActorMessage ActorMsg msg) {
            assertEquals("sender", msg.getSender());
            assertEquals("receiver", msg.getReceiver());
            assertEquals("onMessageWithAnnotation", msg.getTopic());
            assertEquals("Hello", msg.getPayload());
            latch.countDown();
        }
    }

    @Test
    public void testActorMessageAnnotation() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        ActorMsgClass handlerFunctions = new ActorMsgClass(latch);
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(handlerFunctions).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "onMessage", "Hello");
            sender.send("receiver", "onMessageWithAnnotation", "Hello");

            assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    // pub/sub

    private static class Sub {
        private final CountDownLatch latch;

        public Sub(CountDownLatch latch) {
            this.latch = latch;
        }

        @ActorSubscriber
        private void onEvent(String event) {
            assertEquals("Hello subscriber!", event);
            latch.countDown();
        }
    }

    @Test
    public void testPubSub() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Sub sub = new Sub(latch);
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(sub).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.pub("onEvent", "Hello subscriber!");
            assertTrue(latch.await(10, TimeUnit.SECONDS));
    }


    @Test
    public void testPubSubThen() throws InterruptedException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(2);
        Sub sub1 = new Sub(latch);
        Actor receiver1 = Actor.builder().addr("receiver1").setHandlerInstance(sub1).build();
        Sub sub2 = new Sub(latch);
        Actor receiver2 = Actor.builder().addr("receiver2").setHandlerInstance(sub2).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver1)
                .addActor(receiver2)
                .build();


        sender.pubThen("onEvent", "Hello subscriber!").get();
        assertEquals(0, latch.getCount());
    }


    // @ActorScheduler

    private static class SchedulerClass {
        private int count = 0;
        @ActorScheduler(interval = 10L)
        private void onEvent() {
            count++;
        }

        private int getCount() {
            return count;
        }
    }

    @Test
    public void testScheduler () throws InterruptedException {
        SchedulerClass schedulerClass = new SchedulerClass();
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(schedulerClass).build();
        ActorSystem.builder()
                .addActor(receiver)
                .build();
        
            Thread.sleep(100);
            int count = schedulerClass.getCount();
            assertTrue(8 < count && count < 12);

    }
    @Test
    public void testRunDelay () throws InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        Actor actor = Actor.builder().addr("actor").build();
        ActorSystem.builder()
                .addActor(actor)
                .build();
        actor.runDelay(50, TimeUnit.MILLISECONDS, counter::incrementAndGet);
        assertEquals(0, counter.get());
        Thread.sleep(100);
        assertEquals(1, counter.get());
    }

    @Disabled
    @Test
    public void testAddActorScheduler() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        Actor receiver = Actor.builder().addr("receiver")
                .addScheduler(10, TimeUnit.MILLISECONDS,  counter::incrementAndGet)
                .build();
        ActorSystem.builder()
                .addActor(receiver)
                .build();
        Thread.sleep(100);
        int count = counter.get();
        assertTrue(8 < count && count < 12);
    }

    @Test
    public void testRemoveScheduler () throws InterruptedException {
        Actor receiver = Actor.builder().addr("receiver").build();
        AtomicInteger counter = new AtomicInteger();
        ActorSystem.builder()
                .addActor(receiver)
                .build();
        Runnable runnable = counter::incrementAndGet;
        receiver.addActorScheduler(10, TimeUnit.MILLISECONDS, runnable);
        Thread.sleep(100);
        int count = counter.get();
        assertTrue(8 < count && count < 12);

        receiver.removeScheduler( runnable);
        // 等待消息被处理
        Thread.sleep(20);
        // 之后counter不再增长
        count = counter.get();
        Thread.sleep(100);
        assertEquals(count, counter.get());
    }



    @Test
    public void testAddActorListenerWithAnnotation() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(12);
        AnnotationHandlerFunctions handlerFunctions = new AnnotationHandlerFunctions(latch);
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(handlerFunctions).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "noArg");
            sender.sendThen("receiver", "noArgWithReturn").thenAccept(str -> {
                assertEquals("noArgWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "oneArg", "oneArg");
            sender.sendThen("receiver", "oneArgWithReturn", "oneArg").thenAccept(str -> {
                assertEquals("oneArgWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "twoArgs", "twoArgs", 12);
            sender.sendThen("receiver", "twoArgsWithReturn", "twoArgs", 12).thenAccept(str -> {
                assertEquals("twoArgsWithReturn", str);
                latch.countDown();
            });

            sender.send("receiver", "threeArgs", "threeArgs", 12, 1.2f);
            sender.sendThen("receiver", "threeArgsWithReturn", "threeArgs", 12, 1.2f).thenAccept(str -> {
                assertEquals("threeArgsWithReturn", str);
                latch.countDown();
            });
            assertTrue(latch.await(10, TimeUnit.SECONDS));


    }


    @Test
    public void testSetDefaultHandlerFunction() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Actor receiver = Actor.builder().addr("receiver").setDefaultHandlerFunction(msg -> {
            assertEquals("sender", msg.getSender());
            assertEquals("receiver", msg.getReceiver());
            assertEquals("test", msg.getTopic());
            assertEquals(2, msg.getPayloads().length);
            assertEquals("Hello, world!", msg.getPayloads()[0]);
            assertEquals(12, msg.getPayloads()[1]);
            latch.countDown();
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "test", "Hello, world!", 12);
            assertTrue(latch.await(10, TimeUnit.SECONDS));

    }

    @Test
    public void testSetDefaultHandlerFunctionWithTopicHandler() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger topic1Count = new AtomicInteger(0);
        AtomicInteger topic2Count = new AtomicInteger(0);
        Actor receiver = Actor.builder().addr("receiver").addActorListener("topic1", () -> {
            topic1Count.incrementAndGet();
            latch.countDown();
        }).setDefaultHandlerFunction(msg -> {
            assertEquals("sender", msg.getSender());
            assertEquals("receiver", msg.getReceiver());
            assertEquals("topic2", msg.getTopic());
            topic2Count.incrementAndGet();
            latch.countDown();
        }).build();
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic1");
            sender.send("receiver", "topic2");
            assertTrue(latch.await(10, TimeUnit.SECONDS));
            assertEquals(1, topic1Count.get());
            assertEquals(1, topic2Count.get());

    }

    @Test
    public void testResponseHandlerFunction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addActorListener("topic", request -> {
            assertEquals("Hello!", request);
            latch.countDown();
            return "World!";
        }).build();
        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {

            assertEquals("World!", resp.getResult());
            latch.countDown();
        }).build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic", "Hello!");
            assertTrue(latch.await(10, TimeUnit.SECONDS));



    }

    // 注解
    private static class ResponseAnnotationHandlerClass {
        private final CountDownLatch latch;

        public ResponseAnnotationHandlerClass(CountDownLatch latch) {
            this.latch = latch;
        }

        @ActorResponseListener(topic = "my_topic1")
        private void onResponse(ActorMsg response) {
            assertEquals("result", response.getResult());
            assertNull(response.getThrowable());
            latch.countDown();
        }
        @ActorResponseListener(topic = "my_topic2")
        private void onException(ActorMsg response) {
            assertEquals("exception msg", response.getThrowable().getMessage());
            assertNull(response.getResult());
            latch.countDown();
        }
    }

    @Test
    public void testResponseAnnotationFunction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(4);
        Actor receiver = Actor.builder().addr("receiver")
                .addActorListener("my_topic1", () -> {
                    latch.countDown();
                    return "result";
                })
                .addActorListener("my_topic2", () -> {
                    latch.countDown();
                    throw new RuntimeException("exception msg");
                })
                .build();
        ResponseAnnotationHandlerClass cls = new ResponseAnnotationHandlerClass(latch);
        Actor sender = Actor.builder().addr("sender").setResponseHandlerInstance(cls).build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "my_topic1");
            sender.send("receiver", "my_topic2");
            assertTrue(latch.await(10, TimeUnit.SECONDS));


    }


    // 同名方法

    @SuppressWarnings("unused")
    private static class ResponseHandlerClass {
        private final CountDownLatch latch;

        public ResponseHandlerClass(CountDownLatch latch) {
            this.latch = latch;
        }


        private void topicResponse(ActorMsg response) {
            assertEquals("result", response.getResult());
            assertNull(response.getThrowable());
            latch.countDown();
        }

    }

    @Test
    public void testResponseFunction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver")
                .addActorListener("topic", () -> {
                    latch.countDown();
                    return "result";
                })
                .build();
        ResponseHandlerClass cls = new ResponseHandlerClass(latch);
        Actor sender = Actor.builder().addr("sender").setResponseHandlerInstance(cls).build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic");
            assertTrue(latch.await(10, TimeUnit.SECONDS));


    }

    // 默认
    @Test
    public void testDefaultResponseFunction() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(4);
        Actor receiver = Actor.builder().addr("receiver")
                .addActorListener("topic1", () -> {
                    latch.countDown();
                    return "result";
                })
                .addActorListener("topic2", () -> {
                    latch.countDown();
                    return "result";
                })
                .build();
        Actor sender = Actor.builder().addr("sender")
                .setDefaultResponseHandlerFunction(response -> {
                    assertEquals("result", response.getResult());
                    assertNull(response.getThrowable());
                    latch.countDown();
                })
                .build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic1");
            sender.send("receiver", "topic2");
            assertTrue(latch.await(10, TimeUnit.SECONDS));


    }

    // reply

    private static class ReplyCls {
        private final CountDownLatch latch;
        private Actor actor;
        private ReplyCls(CountDownLatch latch) {
            this.latch = latch;
        }

        @ResponseManually
        private void topic1(@ActorMessage ActorMsg msg) {
            assertEquals("Hello", msg.getPayload());
            actor.reply(msg, "World");
            latch.countDown();
        }

        @ResponseManually
        private void topic2(@ActorMessage ActorMsg msg) {
            assertEquals("Hello", msg.getPayload());
            actor.replyException(msg, new RuntimeException("exception msg"));
            latch.countDown();
        }

        public ReplyCls setActor(Actor actor) {
            this.actor = actor;
            return this;
        }
    }

    @Test
    public void testReply() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(4);
        ReplyCls cls = new ReplyCls(latch);
        Actor receiver = Actor.builder().addr("receiver")
                .setHandlerInstance(cls)
                .build();
        cls.setActor(receiver);
        Actor sender = Actor.builder().addr("sender").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.sendThen("receiver", "topic1", "Hello")
                    .thenAccept(result -> {
                        assertEquals("World", result);
                        latch.countDown();
                    });
            sender.sendThen("receiver", "topic2","Hello")
                    .exceptionally(e -> {
                        assertEquals("exception msg", e.getMessage());
                        latch.countDown();
                        return null;
                    });
            assertTrue(latch.await(10, TimeUnit.SECONDS));


    }
    @Test
    public void testSendThen() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addActorListener("topic", request -> {
            assertEquals("Hello!", request);
            latch.countDown();
            return "World!";
        }).build();
        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {

            assertEquals("World!", resp.getResult());
            latch.countDown();
        }).build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        

            sender.send("receiver", "topic", "Hello!");
            assertTrue(latch.await(10, TimeUnit.SECONDS));



    }

    @Test
    public void testResponseConfigIgnored() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addActorListener("topic", request -> {
            assertEquals("Hello!", request);
            latch.countDown();
            return "World";
        }).build();

        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {
            // 响应配置为忽略，不应调用到这里
            latch.countDown();
        }).build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        sender.send("receiver", "topic", ActorMsg.Response.IGNORE, "Hello!");
        assertFalse(latch.await(1, TimeUnit.SECONDS));
        assertEquals(1, latch.getCount());
    }

    @Test
    public void testResponseConfigRequired() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);
        Actor receiver = Actor.builder().addr("receiver").addActorListener("topic", request -> {
            assertEquals("Hello!", request);
            latch.countDown();
        }).build();
        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {

            assertNull(resp.getResult());
            latch.countDown();
        }).build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        sender.send("receiver", "topic", ActorMsg.Response.REQUIRED, "Hello!");
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @SuppressWarnings("SameReturnValue")
    private static class ResponseManuallyCls {
        private Actor actor;
        @ActorListener
        @ResponseManually
        private String topic(@ActorMessage ActorMsg msg) {
            actor.reply(msg, "result1");
            return "result2";
        }

        public void setActor(Actor actor) {
            this.actor = actor;
        }
    }


    @Test
    public void testResponseManually() throws InterruptedException, ExecutionException {
        ResponseManuallyCls cls = new ResponseManuallyCls();
        Actor receiver = Actor.builder().addr("receiver").setHandlerInstance(cls).build();
        cls.setActor(receiver);
        Actor sender = Actor.builder().addr("sender").addResponseHandlerFunction("topic", resp -> {

            assertEquals("result1", resp.getResult());
        }).build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();
        String result = sender.<String>sendThen("receiver", "topic", ActorMsg.Response.REQUIRED, "Hello!").get();
        assertEquals("result1", result);

    }

    @Test
    @Disabled
    public void requestPerformanceTest() throws InterruptedException {
        final int count = 10000000;
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        Actor sender = Actor.builder().addr("sender")
                .enableMetric()
                .build();
        Actor receiver = Actor.builder()
                .addr("receiver")
                .addActorListener("topic", new Consumer<ActorMsg>() {

                    @Override
                    public void accept(@ActorMessage ActorMsg msg) {
                        countDownLatch.countDown();
                    }
                }).build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            sender.send("receiver", "topic", ActorMsg.Response.IGNORE, ActorRejectPolicy.BLOCK, "Hello");
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        logger.info("cost {} ms, {}/s", end - start, count * 1000L / (end - start));

//        msgList.forEach(msg -> logger.info(msg.getContext().getMetric().toString()));

    }
    @Test
    @Disabled
    public void responsePerformanceTest() throws InterruptedException {
        final int count = 10000000;
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        Actor sender = Actor.builder().addr("sender")
//                .enableMetric()
                .build();
        Actor receiver = Actor.builder()
                .addr("receiver")
                .addActorListener("topic", request->"World").build();
        ActorSystem.builder()
                .addActor(sender)
                .addActor(receiver)
                .build();

        long start = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            sender.sendThen("receiver", "topic", ActorRejectPolicy.BLOCK, "Hello").thenAccept(result -> countDownLatch.countDown());
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        logger.info("cost {} ms, {}/s", end - start, count * 1000L / (end - start));

//        msgList.forEach(msg -> logger.info(msg.getContext().getMetric().toString()));

    }
}
