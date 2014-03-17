TweeMemcacheDb
===========

Version 2.0.1 Created by Rostislav Mykhajliw

[![Build Status](https://travis-ci.org/necromant2005/tt-memcachedb.png?branch=master)](https://travis-ci.org/necromant2005/tt-memcachedb)

Introduction
------------

MemcacheDB adapter with Master/Slave replication support

Features / Goals
----------------

* Standard interface zf2 Zend\Cache\Storage\Adapter
* Support master/slave replication
* Support wigth for servers reads
* Support failover 

Installation
------------

### Main Setup

#### With composer

1. Add this to your composer.json:

```json
"require": {
    "necromant2005/tt-memcachedb": "1.*",
}
```

2. Now tell composer to download TweeMemcacheDb by running the command:

```bash
$ php composer.phar update
```

#### Usage

Configuration with 1 master and 2 slaves, due to wieght configuration only 1/5 reads go to master all other 4/5 to slaves. 
Each slave receives 2/5 reads.
```php
use TweeMemcacheDb\Cache\Storage\Adapter;

$options = new MemcacheDbOptions(array(
    array('host' => '127.0.0.1', 'port' => 21201, 'weight' => 1, 'type' => 'master'),
    array('host' => '127.0.0.2', 'port' => 21201, 'weight' => 2, 'type' => 'slave'),
    array('host' => '127.0.0.3', 'port' => 21201, 'weight' => 2, 'type' => 'slave'),
));
$adapter = new MemcacheDb($options);

```