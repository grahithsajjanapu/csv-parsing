package com.grahith.csv.parsing.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "domainRecord")
@Data
public class DomainRecord {

    @Id
    private String domainName;
    private Long rank;
    private boolean isCached;
    private String source;
}
