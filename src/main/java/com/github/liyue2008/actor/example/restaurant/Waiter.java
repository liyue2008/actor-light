package com.github.liyue2008.actor.example.restaurant;

import com.github.liyue2008.actor.Actor;
import com.github.liyue2008.actor.annotation.ActorListener;
import java.util.Map;
public class Waiter {
    private final Actor actor;

    public Waiter(String name) {
        this.actor = Actor.builder()
            .addr("waiter-" + name)
            .setHandlerInstance(this)
            .build();
    }

    public Actor getActor() {
        return actor;
    }


    public void placeOrder(String tableId, Map<String, Integer> foods) {
        actor.<Boolean>sendThen("inventory-manager", "placeOrder", tableId, actor.getAddr(), foods)
            .thenAccept(isSuccess -> {
                if (isSuccess) {
                    System.out.println("place order success, tableId: " + tableId + " foods: " + foods);
                } else {
                    System.out.println("place order failed, tableId: " + tableId + " foods: " + foods);
                }
            });
    }

    @ActorListener
    private void runningFoods(String tableId, Map<String, Integer> foods) {
        System.out.println("Waiter running foods: " + foods + " to table " + tableId);
    }
}

