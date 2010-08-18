<?php
/**
 * Bootstrap file for Unit Testing.
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

define('CO_ROOT_PATH', realpath(dirname(__FILE__) . '/../'));
set_include_path(CO_ROOT_PATH . ':' . get_include_path());
require_once 'PHPUnit/Extensions/Database/TestCase.php';