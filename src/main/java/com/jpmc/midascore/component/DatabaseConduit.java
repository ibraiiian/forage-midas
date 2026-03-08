package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRepository;

    public DatabaseConduit(UserRepository userRepository, TransactionRecordRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    public void save(com.jpmc.midascore.entity.TransactionRecord transactionRecord) {
        transactionRepository.save(transactionRecord);
    }

    public UserRecord getUserById(long id) {
        return userRepository.findById(id);
    }

    public UserRecord getUserByName(String name) {
        return userRepository.findByName(name);
    }
}
