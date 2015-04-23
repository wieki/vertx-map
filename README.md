# Vert.x Asynchronous Mongo DB Persistor

Vert.x module which uses the Asynchronous Mongo DB driver for Java to enable asynchronous calls
to the database, so no worker verticle has to be used. All operations to the database are non-blocking.

## Dependencies 

* This module depends on the [http://www.allanbank.com/mongodb-async-driver/index.html](Async MongoDB Driver) 

## Status

* Currently the module supports find, insert, upsert and delete operations on the database
* Conversion between BSON and JSON types and vice versa (not yet complete)


## Usage

* The module can be configured by providing an configuration object on start-up of the module
* 

## Download

[ ![Download](https://api.bintray.com/packages/socie/vertx-mods/eu.socie.mongo-async-persistor/images/download.svg) ](https://bintray.com/socie/vertx-mods/eu.socie.mongo-async-persistor/_latestVersion) The latest build is available from BinTray.
