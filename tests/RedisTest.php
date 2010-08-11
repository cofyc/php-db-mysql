<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'Redis/DB.php';

class RedisTest extends PHPUnit_Framework_TestCase {

	public function testInstance() {
		$objDB = DB::getInstance();
		$this->assertTrue($objDB instanceof DB);
	}

}