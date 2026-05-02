package com.xormios.device_service;

import com.xormios.device_service.entity.Device;
import com.xormios.device_service.model.DeviceType;
import com.xormios.device_service.repository.DeviceRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Slf4j
class DeviceServiceApplicationTests {

	@Autowired
	private DeviceRepository deviceRepository;

	@Test
	void contextLoads() {
	}

	@Test
	@Disabled
	void createDevices() {
		for(int i = 0; i < 10; i++) {
			Device device = Device.builder()
					.name("Device" + i)
					.type(DeviceType.values()[(int)(Math.random() * DeviceType.values().length)])
					.location("Location" + i)
					.userId((long) ((i % 10) + 1))
					.build();
			deviceRepository.save(device);
		}
		log.info("Devices created");
	}
}
