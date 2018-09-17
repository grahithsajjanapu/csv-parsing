package com.grahith.csv.parsing.redis;

import com.grahith.csv.parsing.exception.ServiceException;
import com.grahith.csv.parsing.model.DomainRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class RedisService {

    private final RedisTemplate<String, DomainRecord> myRedisTemplate;

    public void put(DomainRecord record) {
        try {
            myRedisTemplate.opsForValue().set(record.getDomainName(), record);
        } catch (Exception e) {
            log.warn(e.getMessage());
            log.warn("Unable to write to Redis, Redis-Server may be down");
        }
    }

    public DomainRecord get(String domainName) throws ServiceException {
        try {
            return myRedisTemplate.opsForValue().get(domainName);
        } catch (Exception e) {
            throw new ServiceException("Unable to read from Redis, Redis-Server may be down");
        }
    }
}
