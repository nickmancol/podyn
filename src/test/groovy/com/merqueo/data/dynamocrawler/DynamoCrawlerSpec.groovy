package com.merqueo.data.dynamocrawler

import org.junit.experimental.categories.Category
import spock.lang.Specification
 
@Category(UnitTest.class)
class DynamoCrawlerSpec extends Specification {
 
    def lambda = new Main()

    /**
    PLEASE SET THE ENVIRONMENT VARS 
    DB_URL
    DB_PASS
    DB_USER
    DB_SCHEMA
    DB_TABLES
    REGION
    ACCESS_KEY
    SECRET_KEY
    */
    def 'Get full schema in DWH'() {
        setup:
        HandlerInput input = new HandlerInput()
        when:
        String msg = lambda.handler( input )
        expect:
        msg == "$input.tables synchronized"
    }
}
