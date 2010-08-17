<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once '../MySQL/DB.php';

class MySQLTest extends PHPUnit_Framework_TestCase {

    public function setUp() {
        DB::loadConfigFromFile(CO_ROOT_PATH . '/config.ini');
    }

    /**
     * @dataProvider provider
     */
    public function testSharding($uid, $data) {
        $objDB = DB::getInstance($uid);
        $sql = 'INSERT INTO `user`
        	( `uid`
        	, `data`
        	) VALUE
        	( ' . $objDB->quote($uid) . '
        	, ' . $objDB->quote($data) . '
        	)
		';
        $this->assertTrue(DB::getInstance($uid)->query($sql));
        $sql = 'SELECT * FROM `user`
        	WHERE `uid` = ' . DB::getInstance($uid)->quote($uid) . '
        	LIMIT 1
        ';
        $result = DB::getInstance($uid)->query($sql);
        $this->assertTrue($result instanceof mysqli_result);
        $row = $result->fetch_assoc();
        $this->assertTrue(is_array($row));
        $this->assertTrue((int)$row['uid'] === $uid);
        $this->assertTrue($row['data'] === $data);
    }

    /**
     *
     */
    public function testCaching() {
         $link = new mysqli();
         $link->real_connect('127.0.0.1', 'root', 'root', 'dbtest_shard_index', null, null, MYSQLI_CLIENT_COMPRESS);
         $result = $link->query('SELECT * FROM index_user');
         while ($row = mysqli_fetch_assoc($result)) {
             $objDB = DB::getInstance((int)$row['uid']);
             $this->assertTrue($objDB instanceof DB);
         }
    }

    public function provider() {
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
