<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'Redis/DB.php';

class RedisTest extends PHPUnit_Framework_TestCase {

	public function testInstance() {
	    DB::loadConfigFromFile(CO_ROOT_PATH . '/Redis/config.ini');
		$objDB = DB::getInstance();
		$this->assertTrue($objDB instanceof DB);
		DB::getInstance(1);
	}

}