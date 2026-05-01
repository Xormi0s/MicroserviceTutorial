package com.xormios.user_service.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Aspect
@Component
@Slf4j
public class ExecutionTimeAspect {

    @Pointcut("execution(* com.xormios.user_service.controller.*.*(..))")
    public void controllerMethods(){}

    @Around("controllerMethods()")
    public Object MeasureExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        long startTime = System.nanoTime();

        try {
            return joinPoint.proceed();
        } finally {
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            long durationMs = TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS);
            String signature = joinPoint.getSignature().toShortString();
            log.info("Execution time for {} is {} ms", signature, durationMs);
        }
    }
}
