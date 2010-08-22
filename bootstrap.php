<?php
/**
 * Bootstrap file for Unit Testing.
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

define('DB_INCLUDE_PATH', realpath(dirname(__FILE__) . '/src'));
set_include_path(DB_INCLUDE_PATH . ':' . get_include_path());

$config = array(
    'core' => array(
        'charset' => 'utf8'
    ),
    'global' => array(
        'sqlite:/tmp/db_global.sq3' => array(
            'settings'
        )
    ),
    'sharding' => array(
        'tables' => array(
            'user' => 1
        ),
        'masters' => array(
            1 => 'sqlite:/tmp/db_shard_master.sq3'
        ),
        'memcaches' => array(
            array(
                '127.0.0.1',
                11211
            )
        ),
        'clusters' => array(
            1 => array(
                1 => array(
                    'weight' => '10',
                    'dsn' => 'sqlite:/tmp/db_shard_1.sq3'
                ),
                2 => array(
                    'weight' => '0',
                    'dsn' => 'sqlite:/tmp/db_shard_2.sq3'
                ),
                3 => array(
                    'weight' => '30',
                    'dsn' => 'sqlite:/tmp/db_shard_3.sq3'
                )
            )
        )
    )
);


// CREATE TABLE settings (id INTEGER PRIMARY KEY, name, value);
//
