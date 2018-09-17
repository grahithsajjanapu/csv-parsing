package com.grahith.csv.parsing.repository;

import com.grahith.csv.parsing.model.DomainRecord;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DomainRepository extends MongoRepository<DomainRecord, String> {
}
