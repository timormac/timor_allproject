package jdk;

import java.util.Optional;

/**
 * @Title: A2_Optional
 * @Package: jdk
 * @Description:
 * @Author: lpc
 * @Date: 2024/1/15 18:46
 * @Version:1.0
 */
public class A2_Optional {}

/**
 * 使用Optional更直观
 * map方法表示正常的返回处理
 * orElse方法表示null值的处理方法
 */
class UserServiceOptional {
    public Optional<User> getUserById(int id) {
        // Optional.ofNullable允许传入的参数为null，此时创建一个空的Optional
        if (id == 1) {
            return Optional.of(new User("Alice"));
        } else {
            return Optional.empty();
        }
    }

    public String getUserName(int id) {
        // 使用Optional的map和orElse方法来处理可能为null的情况
        return getUserById(id)
                .map(User::getName)
                .orElse("Unknown");
    }
}

/**
 * 不使用Optional的写法是
 */
class UserService {
    public User getUserById(int id) {
        // 假设这是从数据库中获取用户的方法
        // 如果找不到用户，返回null
        if (id == 1) {
            return new User("Alice");
        } else {
            return null;
        }
    }

    public String getUserName(int id) {
        User user = getUserById(id);
        if (user != null) {
            return user.getName();
        } else {
            return "Unknown";
        }
    }
}

class User {
    private String name;

    public User(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}

