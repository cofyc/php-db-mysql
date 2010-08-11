<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'MySQL/DB.php';

class MySQLTest extends PHPUnit_Framework_TestCase {

	public function testInstance() {
		DB::loadConfigFromFile(CO_ROOT_PATH . '/MySQL/config.ini');
		$objDB = DB::getInstance();
		$this->assertTrue($objDB instanceof DB);
		$rows = DB::getInstance()->query('show status');
		var_dump($rows);die;
	}

}