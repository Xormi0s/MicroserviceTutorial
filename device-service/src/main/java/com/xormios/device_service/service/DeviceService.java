package com.xormios.device_service.service;

import com.xormios.device_service.dto.DeviceDto;
import com.xormios.device_service.entity.Device;
import com.xormios.device_service.repository.DeviceRepository;
import org.springframework.stereotype.Service;

@Service
public class DeviceService {

    DeviceRepository deviceRepository;

    public DeviceService(DeviceRepository deviceRepository) {
        this.deviceRepository = deviceRepository;
    }

    public DeviceDto getDeviceById(Long id) {
        Device device = deviceRepository.findById(id).orElseThrow(() -> new IllegalArgumentException("Device not found with id " + id));
        return mapToDto(device);
    }

    public DeviceDto createDevice(DeviceDto deviceDto) {
        Device device = Device.builder()
                .name(deviceDto.getName())
                .location(deviceDto.getLocation())
                .type(deviceDto.getType())
                .userId(deviceDto.getUserId())
                .build();

        final Device savedDevice = deviceRepository.save(device);
        return mapToDto(savedDevice);
    }

    public DeviceDto updateDeviceById(Long id, DeviceDto deviceDto) {
        Device existingDevice = deviceRepository.findById(id).orElseThrow(() -> new IllegalArgumentException("Device not found with id " + id));

        existingDevice.setName(deviceDto.getName());
        existingDevice.setLocation(deviceDto.getLocation());
        existingDevice.setType(deviceDto.getType());

        final Device updatedDevice = deviceRepository.save(existingDevice);
        return mapToDto(updatedDevice);
    }

    public void deleteDeviceById(Long id) {
        if(!deviceRepository.existsById(id)) {
            throw new IllegalArgumentException("Device not found with id " + id);
        }
        deviceRepository.deleteById(id);
    }

    private DeviceDto mapToDto(Device device){
        DeviceDto deviceDto = DeviceDto.builder()
                .id(device.getId())
                .name(device.getName())
                .type(device.getType())
                .location(device.getLocation())
                .userId(device.getUserId())
                .build();
        return deviceDto;
    }
}
