package com.merqueo.data.dynamocrawler

import com.citusdata.migration.DynamoDBReplicator

class HandlerInput {
    HandlerInput(){
        def env = System.getenv()
        this.dbUrl = env['DB_URL']
        this.dbPass = env['DB_PASS']
        this.dbUser = env['DB_USER']
        this.dbSchema = env['DB_SCHEMA']
        this.dbTables = env['DB_TABLES']
        this.struct = false//env['DB_STRUCT']
        this.aws_region = env['REGION']
        this.aws_access_key = env['ACCESS_KEY']
        this.aws_secret_key = env['SECRET_KEY']
    }

    String dbUrl
    String dbUser
    String dbPass
    String dbSchema
    String dbTables
    boolean struct
    String aws_region
    String aws_access_key
    String aws_secret_key
}

class HandlerOutput {
    String msg
}

class Main {
    public HandlerOutput handler(HandlerInput input) {        
        DynamoDBReplicator.replicate( "$input.dbUrl?sslmode=require&user=$input.dbUser&password=$input.dbPass"
        , input.dbSchema, (input.struct == true), input.dbTables,input.aws_access_key,input.aws_secret_key)
        return new HandlerOutput(msg: "$input.dbTables synchronized" )
    }
}
