package com.github.liyue2008.actor.example.restaurant;

import java.util.HashMap;
import java.util.Map;

import com.github.liyue2008.actor.Actor;
import com.github.liyue2008.actor.annotation.ActorListener;


public class InventoryManager {
    private final Actor actor;

    private final Map<String /* 食材 */, Integer /* 数量 */> inventoryStore = new HashMap<>();

    private final Map<String /* 菜品 */, Map<String /* 食材 */, Integer /* 数量 */>> foodIngredients = new HashMap<>();

    public InventoryManager(Map<String, Integer> inventoryStore, Map<String, Map<String, Integer>> foodIngredients) {
        this.actor = Actor.builder()
            .addr("inventory-manager")
            .setHandlerInstance(this)
            .build();
        this.inventoryStore.putAll(inventoryStore);
        this.foodIngredients.putAll(foodIngredients);
    }

    public Actor getActor() {
        return actor;
    }

    @ActorListener
    private Boolean placeOrder(String tableId, String waiterAddr, Map<String, Integer> foods) {
        Map<String, Integer> ingredients = getIngredientsFromFoods(foods);
        // 检查库存
        if (checkInventory(ingredients)) {
            // 如果库存足够，则处理订单
            // 减去库存
            deductInventory(ingredients);
            // 发送订单给厨师
            actor.send("cook", "placeOrder",tableId, waiterAddr, foods, ingredients);
            return true;
        } else {
            // 如果库存不足，则拒绝订单
            return false;
        }
    }

    private boolean checkInventory(Map<String, Integer> ingredientMap) {

        for (Map.Entry<String, Integer> entry : ingredientMap.entrySet()) {
            String ingredient = entry.getKey();
            Integer ingredientCount = entry.getValue();
            if (inventoryStore.getOrDefault(ingredient, 0) < ingredientCount) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Integer> getIngredientsFromFoods(Map<String, Integer> foods) {
        Map<String, Integer> allIngredients = new HashMap<>();
        for (Map.Entry<String, Integer> entry : foods.entrySet()) {
            String food = entry.getKey();
            Integer count = entry.getValue();
            Map<String, Integer> ingredients = foodIngredients.get(food);
            for (Map.Entry<String, Integer> ingredientEntry : ingredients.entrySet()) {
                String ingredient = ingredientEntry.getKey();
                Integer ingredientCount = ingredientEntry.getValue();
                allIngredients.put(ingredient, allIngredients.getOrDefault(ingredient, 0) + ingredientCount * count);
            }
        }
        return allIngredients;
    }

    /**
     * 扣减库存
     * @param ingredientMap 需要扣减的食材及数量
     */
    private void deductInventory(Map<String, Integer> ingredientMap) {
        for (Map.Entry<String, Integer> entry : ingredientMap.entrySet()) {
            String ingredient = entry.getKey();
            Integer ingredientCount = entry.getValue();
            inventoryStore.put(ingredient, inventoryStore.get(ingredient) - ingredientCount);
        }
    }

}
