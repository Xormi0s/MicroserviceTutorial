package com.xormios.usage_service.client;

import com.xormios.usage_service.dto.DeviceDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;

@Component
public class DeviceClient {

    private final RestTemplate restTemplate = new RestTemplate();
    private final String baseUrl;

    public DeviceClient(@Value("${device.service.url}") String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public DeviceDto getDeviceById(Long deviceId) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl)
                .path("/{deviceId}")
                .buildAndExpand(deviceId)
                .toUriString();

        ResponseEntity<DeviceDto> responseEntity = restTemplate.getForEntity(url, DeviceDto.class);
        return responseEntity.getBody();
    }

    public List<DeviceDto> getAllDevicesForUser(Long userId) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl)
                .path("/user/{userId}")
                .buildAndExpand(userId)
                .toUriString();

        ResponseEntity<DeviceDto[]> responseEntity = restTemplate.getForEntity(url, DeviceDto[].class);
        DeviceDto[] deviceDtos = responseEntity.getBody();
        return deviceDtos == null ? List.of() : List.of(deviceDtos);
    }
}
