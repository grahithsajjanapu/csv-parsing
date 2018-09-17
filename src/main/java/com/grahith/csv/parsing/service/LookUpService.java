package com.grahith.csv.parsing.service;

import com.grahith.csv.parsing.exception.ServiceException;
import com.grahith.csv.parsing.model.DomainRecord;
import com.grahith.csv.parsing.repository.DomainRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class LookUpService {

    private final DomainRepository domainRepository;

    public DomainRecord lookUpDomain(String domainName) throws ServiceException {
        DomainRecord domainRecord = null;
        try {
            domainRecord = domainRepository.findOne(domainName);
            if(domainRecord != null)
                domainRecord.setSource("MONGO");
        } catch (Exception e) {
            throw new ServiceException("Unable to connect to MONGO");
        }
        return domainRecord;
    }
}
