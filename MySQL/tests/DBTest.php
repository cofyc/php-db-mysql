<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once '../MySQL/DB.php';

class DBTest extends PHPUnit_Framework_TestCase {

    public function setUp() {
        DB::loadConfigFromFile(CO_ROOT_PATH . '/config.ini');
    }

    public function tearDown() {}

    /**
     * @dataProvider newUserDataProvider
     */
    public function testNewInsertAndSelect($uid, $data) {
        $objDB = DB::getInstance($uid);
        $sql = 'INSERT INTO `user`
        	( `uid`
        	, `data`
        	) VALUE
        	( ' . $objDB->quote($uid) . '
        	, ' . $objDB->quote($data) . '
        	)
		';
        $this->assertTrue(DB::getInstance($uid)->query($sql) instanceof DB);
        $sql = 'SELECT * FROM `user`
        	WHERE `uid` = ' . DB::getInstance($uid)->quote($uid) . '
        	LIMIT 1
        ';
        $db = DB::getInstance($uid)->query($sql);
        $this->assertTrue($db instanceof DB);
        $row = $db->fetch();
        $this->assertTrue(is_array($row));
        $this->assertTrue((int)$row['uid'] === $uid);
        $this->assertTrue($row['data'] === $data);
    }

    public function testTransfer() {
        $link = new mysqli();
        $link->real_connect('127.0.0.1', 'root', 'root', 'dbtest_shard_index', null, null, MYSQLI_CLIENT_COMPRESS);
        $result = $link->query('SELECT * FROM index_user');
        while ($row = mysqli_fetch_assoc($result)) {
            $uid = (int)$row['uid'];
            $shard_id = (int)$row['shard_id'];
            $locked = (boolean)$row['locked'];
            if ($locked) {
                continue;
            }
            if ($shard_id === 3) {
                DB::startTransfer($uid);
                try {
                    $this->transferUser($uid, 1);
                    DB::endTransfer($uid, 1);
                } catch (Exception $e) {
                    DB::resetTransfer($uid);
                }
            }
        }
    }

    private function transferUser($uid, $to) {
        $sql = 'SELECT * FROM user WHERE uid = ' . DB::getInstance($uid)->quote($uid);
        $rows = DB::getInstance($uid)->query($sql)->fetchAll();
        foreach ($rows as $row) {
            $sql = 'INSERT INTO `user`
            	( `uid`
            	, `data`
            	) VALUE
            	( ' . DB::factoryByShardId($to)->quote($row['uid']) . '
            	, ' . DB::factoryByShardId($to)->quote($row['data']) . '
            	)
    		';
            DB::factoryByShardId($to)->query($sql);
        }
        DB::getInstance($uid)->query('DELETE FROM `user` WHERE uid = ' . DB::getInstance($uid)->quote($uid));
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
