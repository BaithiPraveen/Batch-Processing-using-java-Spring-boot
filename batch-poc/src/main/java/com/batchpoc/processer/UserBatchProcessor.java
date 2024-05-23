package com.batchpoc.processer;

import com.batchpoc.entity.User;
import org.springframework.batch.item.ItemProcessor;

public class UserBatchProcessor implements ItemProcessor<User, User> {

    @Override
    public User process(User item) throws Exception {
        return item;
    }
}
