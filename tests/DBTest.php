<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

class DBTest extends PHPUnit_Framework_TestCase {

    /**
     *
     * @dataProvider settingDataProvider
     */
    public function testGlobalTables($table, $name, $value) {
        $objDB = DB::getInstanceByTableAndShardKey($table);
        $lastInsertedId= $objDB->insert($table)->value(array('name' => $name, 'value' => $value))->query()->lastInsertId();
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
            'anysetingvalue'
        );
        return $data;
    }

    /**
     * @dataProvider newUserDataProvider
     */
    public function testNewInsertAndSelect($uid, $data) {
        $objDB = DB::getInstanceByTableAndShardKey('user', $uid);
        $objDB = $objDB->insert('user', array('uid', 'data'))->value(array($uid, $data))->query();
        $this->assertTrue($objDB instanceof DB);
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
        var_dump(DB::warmUpIndexCache());
        var_dump(DB::warmUpIndexCache(1));
    }
}
