/*
 * Copyright 2015-2018 ForgeRock AS. All Rights Reserved
 *
 * Use of this code requires a commercial software license with ForgeRock AS.
 * or with one of its affiliates. All use shall be exclusively subject
 * to such license between the licensee and ForgeRock AS.
 */

import java.sql.Connection

import org.forgerock.openicf.connectors.groovy.MapFilterVisitor
import org.forgerock.openicf.connectors.groovy.OperationType
import org.forgerock.openicf.connectors.scriptedsql.ScriptedSQLConfiguration
import org.identityconnectors.common.logging.Log
import org.identityconnectors.framework.common.exceptions.ConnectorException
import org.identityconnectors.framework.common.objects.AttributeBuilder
import org.identityconnectors.framework.common.objects.ObjectClass
import org.identityconnectors.framework.common.objects.OperationOptions
import org.identityconnectors.framework.common.objects.SearchResult
import org.identityconnectors.framework.common.objects.filter.Filter

import groovy.sql.Sql

/**
 * Built-in accessible objects
 **/

// OperationType is SEARCH for this script
def operation = operation as OperationType

// The configuration class created specifically for this connector
def configuration = configuration as ScriptedSQLConfiguration

// Default logging facility
def log = log as Log

// The objectClass of the object to be searched, e.g. ACCOUNT or GROUP
def objectClass = objectClass as ObjectClass

// The search filter for this operation
def filter = filter as Filter

// Additional options for this operation
def options = options as OperationOptions

def connection = connection as Connection
def ORG = new ObjectClass("organization")

log.info("HRLite Entering " + operation + " Script");

def sql = new Sql(connection);
def where = " WHERE 1=1 ";
def whereParams = []

def DEPARTMENT = new ObjectClass("department")
//Need to handle the __UID__ and __NAME__ in queries - this map has entries for each objectType,
//and is used to translate fields that might exist in the query object from the ICF identifier
//back to the real property name.
def fieldMap = [
        "__ACCOUNT__" : [
                "__UID__" : "EMPNUM",
                "__NAME__": "EMPNUM",
                "lastName": "LAST_NAME",
                "firstName": "FIRST_NAME",
                "depId": "DEPARTMENT",
                "depName": "NAME",
                "postalCode": "POSTAL_CODE",
                "jobCode": "JOB_CODE",
                "empType": "TYPE",
                "isManager": "IS_MANAGER"
        ],
        "department": [
            "__UID__" : "ID",
            "__NAME__": "ID"
        ]
]

// Set where and whereParams if they have been passed in the request for paging
if (options.pagedResultsCookie != null) {
    def cookieProps = options.pagedResultsCookie.split(",")
    if (cookieProps.size() != 2) {
        throw new ConnectorException("Expecting pagedResultsCookie to contain timestamp and id.")
    }
    // The timestamp and id are for this example only.
    // The user can use their own properties to sort on.
    // For paging it is important that the properties that you use must identify
    // a distinct set of pages for each iteration of the
    // pagedResultsCookie, which can be decided by last record of the previous set.
    where =  " WHERE timestamp >= ? AND EMPNUM > ? "
    whereParams = [cookieProps[0], cookieProps[1].toInteger()]
}

// Determine what properties will be used to sort the query
def orderBy = []
if (options.sortKeys != null && options.sortKeys.size() > 0) {
    options.sortKeys.each {
        def key = it.toString()
        def keySuffix = key.substring(1, key.size())
        def field = fieldMap[objectClass.objectClassValue][keySuffix] ?
            fieldMap[objectClass.objectClassValue][keySuffix] : keySuffix
        if (key.substring(0, 1) == "+") {
            orderBy.add(field + " ASC")
        } else {
            orderBy.add(field + " DESC")
        }
    }
}
if (orderBy) {
    orderBy = " ORDER BY " + orderBy.join(",")
} else {
    orderBy = ""
}

def limit = ""
if (options.pageSize != null) {
    limit = " LIMIT " + options.pageSize.toString()
}

// keep track of lastTimestamp and lastId so we can
// use it for the next request to do paging
def lastTimestamp
def lastId

if (filter != null) {

    def query = filter.accept(MapFilterVisitor.INSTANCE, null)

    // this closure function recurses through the (potentially complex) query object in order to build an equivalent SQL 'where' expression
    def queryParser
    queryParser = { queryObj ->

        if (queryObj.operation == "OR" || queryObj.operation == "AND") {
            return "(" + queryParser(queryObj.right) + " " + queryObj.operation + " " + queryParser(queryObj.left) + ")";
        } else {

            if (fieldMap[objectClass.objectClassValue] && fieldMap[objectClass.objectClassValue][queryObj.get("left")]) {
                queryObj.put("left", fieldMap[objectClass.objectClassValue][queryObj.get("left")]);
            }

            def left = queryObj.get('left')
            def not = queryObj.get('not')
            def template
            switch (queryObj.get('operation')) {
                case 'CONTAINS':
                    template = "$left ${not ? "NOT " : ""}LIKE ?"
                    whereParams.push("%" + queryObj.get("right") + "%")
                    break
                case 'ENDSWITH':
                    template = "$left ${not ? "NOT " : ""}LIKE ?"
                    whereParams.push("%" + queryObj.get("right"))
                    break
                case 'STARTSWITH':
                    template = "$left ${not ? "NOT " : ""}LIKE ?"
                    whereParams.push(queryObj.get("right") + "%")
                    break
                case 'EQUALS':
                    template = "$left ${not ? "<>" : "="} ?"
                    whereParams.push(queryObj.get("right"))
                    break
                case 'GREATERTHAN':
                    template = "$left ${not ? "<=" : ">"} ?"
                    whereParams.push(queryObj.get("right"))
                    break
                case 'GREATERTHANOREQUAL':
                    template = "$left ${not ? "<" : ">="} ?"
                    whereParams.push(queryObj.get("right"))
                    break
                case 'LESSTHAN':
                    template = "$left ${not ? ">=" : "<"} ?"
                    whereParams.push(queryObj.get("right"))
                    break
                case 'LESSTHANOREQUAL':
                    template = "$left ${not ? ">" : "<="} ?"
                    whereParams.push(queryObj.get("right"))
            }
            return template.toString()
        }
    }

    where = where + " AND "+ queryParser(query)
    //log.ok("Search WHERE clause is: " + where)
}
def resultCount = 0
switch (objectClass) {
    case ObjectClass.ACCOUNT:
        def dataCollector = [ uid: "" ]

        def handleCollectedData = {
            if (dataCollector.uid != "") {
                handler {
                    uid dataCollector.id
                    id dataCollector.uid
                    attribute 'firstName', dataCollector.firstName
                    attribute 'email', dataCollector.email
                    attribute 'uid', dataCollector.uid
                    attribute 'depId', dataCollector.depId
                    attribute 'depName', dataCollector.depName
                    attribute 'lastName', dataCollector.lastName
                    attribute 'status', dataCollector.status
                    attribute 'phone', dataCollector.phone
                    attribute 'country', dataCollector.country
                    attribute 'address', dataCollector.address
                    attribute 'city', dataCollector.city
                    attribute 'state', dataCollector.state
                    attribute 'postalCode', dataCollector.postalCode
                    attribute 'empType', dataCollector.empType
                    attribute 'jobCode', dataCollector.jobCode
                    attribute 'empNum', dataCollector.empNum
                    attribute 'userName', dataCollector.userName
                    attribute 'manager', dataCollector.manager
                    attribute 'isManager', dataCollector.isManager
                }

            }
        }

        def statement = """
            SELECT
            u.FIRST_NAME,
            u.EMAIL,
            u.EMPNUM,
            u.DEPARTMENT,
            d.NAME,
            u.LAST_NAME,
            u.STATUS,
            u.PHONE,
            u.COUNTRY,
            u.ADDRESS,
            u.CITY,
            u.STATE,
            u.POSTAL_CODE,
            u.MANAGER,
            u.TYPE,
            u.JOB_CODE,
            u.UPDATED,
            u.IS_MANAGER
            FROM
            users u
            LEFT JOIN departments d
            ON u.department = d.id
            ${where}
            ${orderBy}
            ${limit}
        """

        sql.eachRow(statement, whereParams, { row ->
            if (dataCollector.uid != row.EMPNUM) {
                // new user row, process what we've collected

                handleCollectedData();

                dataCollector = [
                        id : row.EMPNUM as String,
                        uid : row.EMPNUM as String,
                        email: row.EMAIL,
                        depId: row.DEPARTMENT,
                        depName: row.NAME,
                        firstName : row.FIRST_NAME,
                        lastName: row.LAST_NAME,
                        status: row.STATUS,
                        phone: row.PHONE,
                        country: row.COUNTRY,
                        address: row.ADDRESS,
                        city: row.CITY,
                        state: row.STATE,
                        postalCode: row.POSTAL_CODE,
                        manager: row.MANAGER,
                        empType: row.TYPE,
                        jobCode: row.JOB_CODE,
                        empNum: row.EMPNUM,
                        isManager: row.IS_MANAGER,
                        userName: (row.FIRST_NAME.substring(0,1)+row.LAST_NAME).toLowerCase()
                ]
            }


            lastTimestamp = row.UPDATED
            lastId = row.EMPNUM
            resultCount++
        });

        handleCollectedData();

        break

    
    
    case DEPARTMENT: 
      def statement = """
        SELECT id, parent_id, name, description, updated
        FROM departments ${where} ${orderBy} ${limit}
      """
      sql.eachRow(statement, whereParams, { row ->
        handler {
          uid row.id
          id row.id as String
          attribute 'parentId', row.parent_id
          attribute 'name', row.name
          attribute 'description', row.description
        }

        lastTimestamp = row.updated
        lastId = row.id
        resultCount++
      })
      break
    default:
        throw new UnsupportedOperationException(operation.name() + " operation of type:" +
                objectClass.objectClassValue + " is not supported.")
}

// If paging is not wanted just return the default SearchResult object
if (orderBy.toString().isEmpty() || limit.toString().isEmpty() || resultCount < options.pageSize) {
    return new SearchResult();
}

return new SearchResult(lastTimestamp.toString() + "," + lastId.toString(), -1);
