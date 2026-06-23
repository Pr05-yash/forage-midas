package com.jpmc.midascore;

import com.jpmc.midascore.entity.User;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {

    @Autowired
    private UserRepository userRepository;

    @GetMapping(value = "/balance")
    public Balance getBalance(@RequestParam(name = "userId") Long userId) {
        User user = userRepository.findById(userId).orElse(null);
        return (user == null) ? new Balance(0.0) : new Balance(user.getBalance());
    }
}
