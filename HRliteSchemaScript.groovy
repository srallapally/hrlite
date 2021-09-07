/*
 * Copyright 2015-2018 ForgeRock AS. All Rights Reserved
 *
 * Use of this code requires a commercial software license with ForgeRock AS.
 * or with one of its affiliates. All use shall be exclusively subject
 * to such license between the licensee and ForgeRock AS.
 */

import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.MULTIVALUED
import static org.identityconnectors.framework.common.objects.AttributeInfo.Flags.REQUIRED

import org.forgerock.openicf.connectors.groovy.ICFObjectBuilder
import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.scriptedsql.ScriptedSQLConfiguration
import org.identityconnectors.common.logging.Log

/**
 * Built-in accessible objects
 **/

// OperationType is SCHEMA for this script
def operation = operation as OperationType

// The configuration class created specifically for this connector
def configuration = configuration as ScriptedSQLConfiguration

// Default logging facility
def log = log as Log

// The schema builder object
def builder = builder as ICFObjectBuilder

/**
 * Script action - Customizable
 *
 * Build the schema for this connector that describes what the ICF client will see.  The schema
 * might be statically built or may be built from data retrieved from the external source.
 *
 * This script should use the builder object to create the schema.
 **/
/* Log something to demonstrate this script executed */

//log.info("HRLite Entering " + operation + " Script");

builder.schema({
    objectClass {
        type ObjectClass.ACCOUNT_NAME
        attributes {
            uid String.class, REQUIRED
            firstName String.class, REQUIRED
            lastName String.class, REQUIRED
            email String.class, REQUIRED
            depId String.class, REQUIRED
            depName String.class, REQUIRED
            status String.class, REQUIRED
            phone String.class, REQUIRED
            country String.class, REQUIRED
            address String.class, REQUIRED
            city String.class, REQUIRED
            state String.class, REQUIRED
            postalCode String.class, REQUIRED
            empType String.class, REQUIRED
            jobCode String.class, REQUIRED
            isManager String.class, REQUIRED
        }
    }
    objectClass {
      type 'department'
      attributes {
        uid String.class, REQUIRED
        parent String.class, REQUIRED
        name String.class, REQUIRED
        description String.class, REQUIRED
      }
    }
})
