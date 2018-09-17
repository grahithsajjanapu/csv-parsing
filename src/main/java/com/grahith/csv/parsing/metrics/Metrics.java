package com.grahith.csv.parsing.metrics;

import com.codahale.metrics.MetricRegistry;
import com.grahith.csv.parsing.model.DomainRecord;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.annotation.AfterReturning;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class Metrics {

    private static final String REDIS_HITS = "Redis.Hits";
    private static final String MONGO_HITS = "Mongo.Hits";
    private static final String REDIS_MISSES = "Redis.Misses";
    private static final String MONGO_MISSES = "Mongo.Misses";
    private static final String DUPLICATE_KEY_EXCEPTION = "DuplicateKeyException";

    private MetricRegistry metricRegistry;

    public void init() {
        metricRegistry.counter(REDIS_HITS);
        metricRegistry.counter(MONGO_HITS);
        metricRegistry.counter(REDIS_MISSES);
        metricRegistry.counter(MONGO_MISSES);
        metricRegistry.counter(DUPLICATE_KEY_EXCEPTION);
    }

    @AfterReturning(pointcut = "execution( * com.gratith.csv.parsing.redis.RedisService.get(..))", returning = "domainRecord")
    public void interceptCache(DomainRecord domainRecord) {
        if (domainRecord != null)
            metricRegistry.counter(REDIS_HITS).inc();
        else
            metricRegistry.counter(REDIS_MISSES).inc();
    }

    @AfterReturning(pointcut = "execution( * com.gratith.csv.parsing.service.LookUpService.lookUpDomain(String))", returning = "domainRecord")
    public void interceptMongo(DomainRecord domainRecord) {
        if (domainRecord != null)
            metricRegistry.counter(MONGO_HITS).inc();
        else
            metricRegistry.counter(MONGO_MISSES).inc();
    }

    public void duplicateKeyExceptionCount(){
        metricRegistry.counter(DUPLICATE_KEY_EXCEPTION).inc();
    }
}
