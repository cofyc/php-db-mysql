<?php

define('CO_ROOT_PATH', realpath(dirname(__FILE__) . '/../'));

require_once '../DB.php';

DB::loadConfigFromFile(CO_ROOT_PATH . '/config.ini');

$rows = DB::getInstance(101027926)->query('select * from user where uid = 101027926')->fetchAll();
var_dump($rows);
$rows = DB::getInstance(101027926)->prepare('select * from user where uid = 101027926');
var_dump($rows);
