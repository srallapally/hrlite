/*
 * Copyright 2015-2018 ForgeRock AS. All Rights Reserved
 *
 * Use of this code requires a commercial software license with ForgeRock AS.
 * or with one of its affiliates. All use shall be exclusively subject
 * to such license between the licensee and ForgeRock AS.
 */

import java.sql.Connection

import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.scriptedsql.ScriptedSQLConfiguration
import org.identityconnectors.common.logging.Log
import org.identityconnectors.framework.common.exceptions.ConnectorException
import org.identityconnectors.framework.common.objects.Attribute
import org.identityconnectors.framework.common.objects.AttributesAccessor
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.Uid

import groovy.sql.Sql

/**
 * Built-in accessible objects
 **/

// OperationType is UPDATE for this script
def operation = operation as OperationType

// The configuration class created specifically for this connector
def configuration = configuration as ScriptedSQLConfiguration

// Default logging facility
def log = log as Log

// Set of attributes describing the object to be updated
def updateAttributes = new AttributesAccessor(attributes as Set<Attribute>)

// The Uid of the object to be updated
def uid = uid as Uid

// The objectClass of the object to be updated, e.g. ACCOUNT or GROUP
def objectClass = objectClass as ObjectClass

/**
 * Script action - Customizable
 *
 * Update an object in the external source.  Connectors that do not support this should
 * throw an UnsupportedOperationException.
 *
 * This script should return the Uid of the updated object
 **/

/* Log something to demonstrate this script executed */

//log.info("HRLite Entering " + operation + " Script");

def connection = connection as Connection
def sql = new Sql(connection)
def ORG = new ObjectClass("organization")

switch (operation) {
    case OperationType.UPDATE:
        switch (objectClass) {
            case ObjectClass.ACCOUNT:
                sql.executeUpdate("""
                        UPDATE
                            users
                        SET
                            email = ?
                        WHERE
                            empnum = ?
                        """,
                        [
                                updateAttributes.findString("email"),
                                uid.uidValue
                        ]
                );
                break
            default:
                throw new ConnectorException("UpdateScript can not handle object type: " + objectClass.objectClassValue)
        }
        return uid.uidValue
    default:
        throw new ConnectorException("UpdateScript can not handle operation:" + operation.name())
}
