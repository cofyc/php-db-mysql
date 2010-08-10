<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'MySQL/DB.php';

class MySQLTest extends PHPUnit_Framework_TestCase {

	public function testInstance() {
		$objDB = DB::getInstance();
		$this->assertTrue($objDB instanceof DB);
	}

}