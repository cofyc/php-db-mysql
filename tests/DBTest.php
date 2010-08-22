<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'DB.php';

class DBTest extends PHPUnit_Framework_TestCase {

    public function setUp() {
        global $config;
        DB::setConfig($config);
    }

    /**
     *
     * @dataProvider settingDataProvider
     */
    public function testGlobalTables($table, $name, $value) {
        $objDB = DB::getInstanceByTableAndShardKey($table);
        $sql = 'INSERT INTO ' . $table . ' (name, value)
        	VALUES
        	( ' . $objDB->quote($name) . '
        	, ' . $objDB->quote($value) . '
        	)
		';
        $this->assertTrue($objDB->exec($sql) === 1);
        $lastInsertedId = $objDB->lastInsertId();
        $sql = "DELETE FROM " . $table . "
        	WHERE id = " . $objDB->quote($lastInsertedId) . "
		";
        $this->assertTrue($objDB->exec($sql) === 1);
    }

    public function settingDataProvider() {
        $data = array();
        $data[] = array(
            'settings',
            'asettingname',
            'anysetingvalue',
        );
        return $data;
    }

    public function newUserDataProvider() {
        $data = array();
        for ($i = 0; $i < 100; $i++) {
            $data[] = array(
                mt_rand(1, mt_getrandmax()),
                sha1(mt_rand())
            );
        }
        return $data;
    }
}