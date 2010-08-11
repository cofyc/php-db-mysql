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
		$mysqli_result = DB::getInstance()->query('show status');
		$result1 = mysqli_num_rows($mysqli_result);
		$mysqli_result = DB::getInstance()->setMaster(true);
		$mysqli_result = DB::getInstance()->query('show status');
		$result2 = mysqli_num_rows($mysqli_result);
		$this->assertTrue($result1 !== $result2);
	}

}