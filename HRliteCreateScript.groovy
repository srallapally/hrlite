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
import org.identityconnectors.framework.common.objects.Attribute
import org.identityconnectors.framework.common.objects.AttributesAccessor
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.Uid

import groovy.sql.Sql

/**
 * Built-in accessible objects
 **/

// OperationType is CREATE for this script
def operation = operation as OperationType

// The configuration class created specifically for this connector
def configuration = configuration as ScriptedSQLConfiguration

// Default logging facility
def log = log as Log

// Set of attributes describing the object to be created
def createAttributes = new AttributesAccessor(attributes as Set<Attribute>)

// The Uid of the object to be created, usually null indicating the Uid should be generated
def uid = id as String

// The objectClass of the object to be created, e.g. ACCOUNT or GROUP
def objectClass = objectClass as ObjectClass

/**
 * Script action - Customizable
 *
 * Create a new object in the external source.  Connectors that do not support this should
 * throw an UnsupportedOperationException.
 *
 * This script should return a Uid object that represents the ID of the newly created object
 **/

/* Log something to demonstrate this script executed */
//log.info("HRLite Entering " + operation + " Script");

def ORG = new ObjectClass("organization")
def connection = connection as Connection
def sql = new Sql(connection);

switch (objectClass) {
    case ObjectClass.ACCOUNT:
        def retUid
        def generatedKeys = sql.executeInsert(
                "INSERT INTO users (FIRST_NAME,LAST_NAME,EMAIL,PHONE) values (?,?,?,?)",
                [
                        uid,
                        createAttributes.hasAttribute("lastName") ? createAttributes.findString("lastName") : "",
                        createAttributes.hasAttribute("email") ? createAttributes.findString("email") : "",
                        createAttributes.hasAttribute("phone") ? createAttributes.findString("phone") : ""
                ]
        )

        /*
        def generatedKeys = sql.executeInsert(
                "INSERT INTO users (FIRST_NAME,LAST_NAME,DEPARTMENT,JOB_CODE,EMAIL,TYPE,STATUS,PHONE,COUNTRY,ADDRESS,CITY,STATE,POSTAL_CODE) values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                        createAttributes.hasAttribute("firstName") ? createAttributes.findString("firstName") : "",
                        createAttributes.hasAttribute("lastName") ? createAttributes.findString("lastName") : "",
                        createAttributes.hasAttribute("department") ? createAttributes.findString("department") : "",
                        createAttributes.hasAttribute("jobCode") ? createAttributes.findString("jobCode") : "",
                        createAttributes.hasAttribute("email") ? createAttributes.findString("email") : "",
                        createAttributes.hasAttribute("empType") ? createAttributes.findString("empType") : "",
                        createAttributes.hasAttribute("status") ? createAttributes.findString("status") : "",
                        createAttributes.hasAttribute("phone") ? createAttributes.findString("phone") : "",
                        createAttributes.hasAttribute("country") ? createAttributes.findString("country") : "",
                        createAttributes.hasAttribute("address") ? createAttributes.findString("address") : "",
                        createAttributes.hasAttribute("city") ? createAttributes.findString("city") : "",
                        createAttributes.hasAttribute("state") ? createAttributes.findString("state") : "",
                        createAttributes.hasAttribute("postalCode") ? createAttributes.findString("postalCode") : ""
                ]
        )
        */

        retUid = new Uid(generatedKeys[0][0] as String)

        return retUid
        break

    default:
        throw new UnsupportedOperationException(operation.name() + " operation of type:" +
                objectClass.objectClassValue + " is not supported.")
}
