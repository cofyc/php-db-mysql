<?php
/**
 * Bootstrap file for Unit Testing.
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

define('CO_ROOT_PATH', realpath(dirname(__FILE__) . '/../'));
set_include_path(CO_ROOT_PATH . ':' . get_include_path());

$config = array(
    'core' => array(
        'charset' => 'utf8'
    ),
    'global' => array(
        'mysql://root:root@127.0.0.1:3306/dbtest_global' => array(
            'settings'
        )
    ),
    'sharding' => array(
        'tables' => array(
            'user' => 1
        ),
        'masters' => array(
            1 => array(
                'dsn' => 'mysql://root:root@127.0.0.1:3306/dbtest_shard_index',
                'memcache_host' => '127.0.0.1',
                'memcache_port' => 11211
            )
        ),
        'clusters' => array(
            1 => array(
                1 => array(
                    'weight' => '10',
                    'dsn' => 'mysql://root:root@127.0.0.1:3306/dbtest_shard_1'
                ),
                2 => array(
                    'weight' => '0',
                    'dsn' => 'mysql://root:root@127.0.0.1:3306/dbtest_shard_2'
                ),
                3 => array(
                    'weight' => '30',
                    'dsn' => 'mysql://root:root@127.0.0.1:3306/dbtest_shard_3'
                )
            )
        )
    )
);