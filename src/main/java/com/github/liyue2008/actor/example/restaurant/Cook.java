package com.github.liyue2008.actor.example.restaurant;

import com.github.liyue2008.actor.Actor;
import com.github.liyue2008.actor.annotation.ActorListener;
import java.util.Map;
public class Cook {
    private final Actor actor;

    public Cook() {
        this.actor = Actor.builder()
            .addr("cook")
            .setHandlerInstance(this)
            .build();
    }

    public Actor getActor() {
        return actor;
    }

    @ActorListener
    private void placeOrder(String tableId, String waiterAddr, Map<String, Integer> foods, Map<String, Integer> ingredients) throws InterruptedException {

        
        for (Map.Entry<String, Integer> entry : foods.entrySet()) {
            String food = entry.getKey();
            Integer count = entry.getValue();
            System.out.println("Cook is cooking " + food + " " + count);
            Thread.sleep(1000); // 模拟烹饪时间
        }

        actor.send(waiterAddr, "runningFoods", tableId, foods);

    }
}


