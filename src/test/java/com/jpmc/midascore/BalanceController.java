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

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam(name = "userId") Long userId) {
        // User ko database se dhoondein
        User user = userRepository.findById(userId).orElse(null);
        
        // Agar user nahi mila, toh balance 0 return karein
        if (user == null) {
            return new Balance(0.0);
        }
        
        // Agar user mila, toh uska balance return karein
        return new Balance(user.getBalance());
    }
}
