package com.xormios.user_service;

import com.xormios.user_service.entity.User;
import com.xormios.user_service.repository.UserRepository;
import com.xormios.user_service.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Slf4j
class UserServiceApplicationTests {

	@Autowired
	private UserRepository userRepository;

	@Test
	void contextLoads() {
	}

	@Test
	@Disabled
	void createUsers() {
		for(int i = 0; i < 10; i++) {
			User user = User.builder()
					.firstName("User" + i)
					.lastName("User Lastname" + i)
					.email("user" + i + "@testing.com")
					.address("Street " + i)
					.alerting(i % 2 == 0)
					.energyAlertingThreshold(100 + i)
					.build();
			userRepository.save(user);
		}
		log.info("Users created");
	}
}
