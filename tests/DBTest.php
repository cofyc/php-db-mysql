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
        $lastInsertedId = $objDB->query($sql)->lastInsertId();
        $sql = "DELETE FROM " . $table . "
        	WHERE id = " . $objDB->quote($lastInsertedId) . "
		";
        $objDB->query($sql);
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

    /**
     * @dataProvider newUserDataProvider
     */
    public function testNewInsertAndSelect($uid, $data) {
        $objDB = DB::getInstanceByTableAndShardKey('user', $uid);
        $sql = 'INSERT INTO `user`
        	( `uid`
        	, `data`
        	) VALUE
        	( ' . $objDB->quote($uid) . '
        	, ' . $objDB->quote($data) . '
        	)
		';
        $this->assertTrue($objDB->query($sql) instanceof DB);
        $sql = 'SELECT * FROM `user`
        	WHERE `uid` = ' . $objDB->quote($uid) . '
        	LIMIT 1
        ';
        $db = $objDB->query($sql);
        $this->assertTrue($db instanceof DB);
        $row = $db->fetch();
        $this->assertTrue(is_array($row));
        $this->assertTrue((int)$row['uid'] === $uid);
        $this->assertTrue($row['data'] === $data);
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

    public function testWarmUpIndexCacher() {
        $stats = DB::warmUpIndexCache(1);
        var_dump($stats);
//        $stats = DB::warmUpIndexCache(2);
//        var_dump($stats);
    }
}
