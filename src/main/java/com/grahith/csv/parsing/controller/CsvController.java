package com.grahith.csv.parsing.controller;

import com.grahith.csv.parsing.exception.ServiceException;
import com.grahith.csv.parsing.redis.RedisService;
import com.grahith.csv.parsing.exception.ClientRequestException;
import com.grahith.csv.parsing.model.DomainRecord;
import com.grahith.csv.parsing.service.LookUpService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.validator.routines.DomainValidator;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
public class CsvController {

    private static final String DOMAIN_NAME_PATTERN = "[a-zA_Z0-9.-]+\\.[a-zA_Z]{2,}$";

    private final LookUpService lookUpService;

    private final RedisService redisService;

    @GetMapping(value = "csv/domain/{domainName:" + DOMAIN_NAME_PATTERN + "}",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public DomainRecord get(@PathVariable String domainName) throws ClientRequestException {
        DomainRecord domainRecord = lookup(domainName.toLowerCase());
        if (domainRecord == null)
            throw new ClientRequestException("Domain '" + domainName + "' not found");
        return domainRecord;
    }

    private DomainRecord lookup(String domain) throws ClientRequestException {
        DomainValidator domainValidator = DomainValidator.getInstance();
        if (!domainValidator.isValid(domain))
            throw new ClientRequestException("' " + domain + "' is not a valid Domain");

        DomainRecord domainRecord = null;

        try {
            domainRecord = redisService.get(domain);
        } catch (ServiceException e) {
            log.error(e.getMessage());
        }
        if (domainRecord != null) {
            domainRecord.setCached(true);
        }

        if (domainRecord == null) {
            try {
                domainRecord = lookUpService.lookUpDomain(domain);
            } catch (ServiceException e) {
                log.error(e.getMessage());
            }

            if (domainRecord != null) {
                redisService.put(domainRecord);
                domainRecord.setCached(false);
            }
        }
        return domainRecord;
    }
}
