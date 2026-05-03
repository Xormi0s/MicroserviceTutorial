package com.xormios.usage_service.client;

import com.xormios.usage_service.dto.UserDto;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

@Component
public class UserClient {

    private final RestTemplate restTemplate = new RestTemplate();
    private final String baseUrl;

    public UserClient(@Value("${user.service.url}") String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public UserDto getUserById(Long userId) {
        String url = UriComponentsBuilder
                .fromUriString(baseUrl)
                .path("/{userId}")
                .buildAndExpand(userId)
                .toUriString();

        ResponseEntity<UserDto> responseEntity = restTemplate.getForEntity(url, UserDto.class);
        return responseEntity.getBody();
    }
}
