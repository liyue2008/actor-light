package com.github.liyue2008.actor.example.restaurant;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.liyue2008.actor.ActorSystem;

public class EggTomatoRestaurant {

    public static void main(String[] args) {
        // 创建Actor
        Cook cook = new Cook();
        // 创建库存管理员，初始库存：10个鸡蛋和10个番茄，番茄炒蛋配方：每份番茄炒蛋需要3个鸡蛋和2个番茄
        InventoryManager inventoryManager = new InventoryManager(createInventoryStore(), createFoodIngredients());
        List<Waiter> waiters = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Waiter waiter = new Waiter("waiter-" + i);
            waiters.add(waiter);
        }
        // 创建并启动ActorSystem
        ActorSystem.builder()
            .addActors(waiters.stream().map(Waiter::getActor).collect(Collectors.toList()))
            .addActor(cook.getActor())
            .addActor(inventoryManager.getActor())
            .name("egg-tomato-restaurant")
            .threadCount(5)
            .build();
        // 模拟5个顾客同时点餐，每人点一份番茄炒蛋
        IntStream.range(0, 5).parallel().forEach(i -> {
            Waiter waiter = waiters.get(i);
            waiter.placeOrder("table-" + i, createFoodOrder());
        });
    }

    private static Map<String, Integer> createFoodOrder() {
        Map<String, Integer> foodOrder = new HashMap<>();
        foodOrder.put("egg-tomato", 1);
        return foodOrder;
    }

    private static Map<String, Map<String, Integer>> createFoodIngredients() {
        // 番茄炒蛋配方，每份需要3个鸡蛋和2个番茄
        Map<String, Map<String, Integer>> foodIngredients = new HashMap<>();
        Map<String, Integer> eggTomato = new HashMap<>();
        eggTomato.put("egg", 3);
        eggTomato.put("tomato", 2);
        foodIngredients.put("egg-tomato", eggTomato);

        return foodIngredients;
    }

    private static Map<String, Integer> createInventoryStore() {
        // 初始库存，10个鸡蛋和10个番茄
        Map<String, Integer> inventoryStore = new HashMap<>();
        inventoryStore.put("egg", 10);
        inventoryStore.put("tomato", 10);
        return inventoryStore;
    }
}