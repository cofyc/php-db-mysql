<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'DBQueryBuilder.php';

class DB extends DBQueryBuilder {

    /**
     *
     * @var DB
     */
    private static $instances = array();

    /**
     *
     * @var array
     */
    private static $config;

    /**
     *
     * @var mysqli
     */
    private $link;

    /**
     * Link pool
     *
     * @var array, array of mysqlis
     */
    private static $links = array();

    /**
     *
     * @var string
     */
    private $host;

    /**
     *
     * @var integer
     */
    private $port;

    /**
     *
     * @var string
     */
    private $username;

    /**
     *
     * @var string
     */
    private $passwd;

    /**
     *
     * @var string
     */
    private $dbname;

	/**
     *
     * @var string
     */
    private $link_key;


    /**
     *
     * @var string
     */
    private $sql;

    /**
     *
     * @var mysqli_result
     */
    private $result;

    /**
     * Current shard cluster id.
     * @var integer
     */
    private static $shard_cluster_id;

    /**
     *
     * @var Memcached
     */
    private static $objShardingIndexCacher;

    /**
     *
     * @var DB
     */
    private static $objShardingMaster;

    /**
     *
     * @var array
     */
    private static $shard_indices = array();

    /**
     *
     * @param string $dsn
     * @return DB
     */
    private function __construct($dsn) {
        $infos = self::parseDSN($dsn);
        if (!$infos) {
            throw new Exception('dsn format is wrong');
        }

        $this->host = $infos['host'];
        $this->port = $infos['port'];
        $this->username = $infos['username'];
        $this->passwd = $infos['passwd'];
        $this->dbname = $infos['dbname'];
        $this->link_key = $this->host . $this->port;

        return $this;
    }

    /**
     *
     * @param string $table
     * @param integer $shard_key, optional
     * @throws Exception
     * @return DB
     */
    private static function factory($table, $shard_key = null) {
        if (!is_string($table)) {
            throw new Exception('table should be string');
        }
        foreach (self::getConfig('global', array()) as $dsn => $tables) {
            if (in_array($table, $tables)) {
                return new self($dsn);
            }
        }

        if (!isset($shard_key)) {
            throw new Exception('table is not in global tables, shard_key must be provided');
        }

        // sharding
        try {
            $shard = self::sharding($table, $shard_key);
        } catch (Exception $e) {
            throw $e;
        }

        return new self($shard['dsn']);
    }

    /**
     *
     * @param integer $shard_cluster_id
     * @param integer $shard_id
     * @throws Exception
     * @return DB
     */
    public static function factoryByShardClusterIdAndShardId($shard_cluster_id, $shard_id) {
        if (!is_int($shard_cluster_id)) {
            throw new Exception();
        }
        if (!is_int($shard_id)) {
            throw new Exception();
        }
        $sharding_clusters = self::getConfig('sharding.clusters');
        if (!isset($sharding_clusters[$shard_cluster_id][$shard_id])) {
            throw new Exception();
        }
        return new self($sharding_clusters[$shard_cluster_id][$shard_id]['dsn']);
    }

    /**
     *
     * @param string $table
     * @param integer $shard_key
     * @throws Exception
     * @return array $shard
     */
    private static function sharding($table, $shard_key) {
        if (!is_string($table)) {
            throw new Exception();
        }
        if (!is_int($shard_key)) {
            throw new Exception();
        }

        $sharding_tables = self::getConfig('sharding.tables', array());
        if (!isset($sharding_tables[$table])) {
            throw new Exception();
        }
        self::$shard_cluster_id = $sharding_tables[$table];

        $shards = self::getConfig(sprintf('sharding.clusters.%d', self::$shard_cluster_id), array());
        if (!isset(self::$shard_indices[$shard_key])) {
            self::xShardingIndexCacher();

            $shard_id = self::$objShardingIndexCacher->get(self::getIndexCacheKey($shard_key));
            if ($shard_id === false) {
                self::xShardingMaster();

                // try to read from index db
                try {
                    $row = self::$objShardingMaster->query("SELECT shard_id FROM index_user WHERE uid = " . (int)$shard_key)->fetch();
                    if (!$row || !isset($row['shard_id'])) {
                        throw new Exception('failed to get shard id');
                    }
                    $shard_id = (int)$row['shard_id'];
                } catch (Exception $e) {
                    // or random allocates and stores it right now
                    $shard_id = self::randomSharding($shards);
                    $sql = 'INSERT INTO index_user
                    	( `uid`
                    	, `shard_id`
                    	) VALUE
                    	( ' . (int)$shard_key . '
                    	, ' . (int)$shard_id . '
                    	)
    				';
                    try {
                        self::$objShardingMaster->query($sql);
                    } catch (Exception $e) {
                        throw new Exception('failed to insert index');
                    }
                }

                self::$objShardingIndexCacher->set(self::getIndexCacheKey($shard_key), $shard_id);
            }

            self::$shard_indices[$shard_key] = $shard_id;
        }

        if (!isset($shards[self::$shard_indices[$shard_key]])) {
            throw new Exception('shard id does not exist');
        }

        return $shards[self::$shard_indices[$shard_key]];
    }

    /**
     *
     * @param array $shards
     * @throws Exception
     * @return integer
     */
    private static function randomSharding($shards) {
        if (!is_array($shards)) {
            throw new Exception();
        }
        $total_weight = 0;
        foreach ($shards as $shard_id => $shard) {
            $total_weight += $shard['weight'];
            $shards[$shard_id]['tmp_weight'] = $total_weight;
        }
        $random_weight = mt_rand(0, $total_weight - 1);
        foreach ($shards as $shard_id => $shard) {
            if ($random_weight < $shard['tmp_weight']) {
                return $shard_id;
            }
        }
        throw new Exception('failed');
    }

    /**
     *
     * @param string $table
     * @param integer $shard_key, optional
     * @throws Exception
     * @return DB
     */
    public static function getInstance($table, $shard_key = null) {
        $unique_key = sprintf('%s/%d', $table, $shard_key);
        if (!isset(self::$instances[$unique_key])) {
            self::$instances[$unique_key] = self::factory($table, $shard_key);
        }
        return self::$instances[$unique_key];
    }

    /**
     * Index Cache Warm-Up
     *
     * @param integer $shard_cluster_id
     * @throws Exception
     * @return array
     * - total: integer
     * - cached: integer
     * - ignored: integer
     */
    public static function warmUpIndexCache($shard_cluster_id) {
        self::$shard_cluster_id = $shard_cluster_id;

        self::xShardingMaster();
        self::xShardingIndexCacher();

        try {
            self::$objShardingMaster->query('SELECT uid, locked, shard_id FROM index_user');
        } catch (Exception $e) {
            throw new Exception('faild to read index from db');
        }

        $stats = array(
            'total' => 0,
            'cached' => 0,
            'failed' => 0,
            'locaked' => 0
        );
        while ($row = self::$objShardingMaster->fetch()) {
            $stats['total']++;
            if ($row['locked']) {
                $stats['locaked']++;
                continue;
            }

            if (!self::$objShardingIndexCacher->set(self::getIndexCacheKey((int)$row['uid']), $row['shard_id'])) {
                $stats['failed']++;
            } else {
                $stats['cached']++;
            }
        }

        return $stats;
    }

    /**
     *
     * @throws Exception
     * @return void
     */
    private static function xShardingMaster() {
        if (!isset(self::$shard_cluster_id)) {
            throw new Exception();
        }
        static $objs = array();
        if (!isset($objs[self::$shard_cluster_id])) {
            $shard_master = self::getConfig(sprintf('sharding.masters.%d', self::$shard_cluster_id));
            if (!is_array($shard_master)) {
                throw new Exception();
            }
            $objs[self::$shard_cluster_id] = new self($shard_master['dsn']);
        }

        self::$objShardingMaster = $objs[self::$shard_cluster_id];
    }

    /**
     *
     * @throws Exception
     * @return void
     */
    private static function xShardingIndexCacher() {
        if (!isset(self::$shard_cluster_id)) {
            throw new Exception();
        }
        static $objs = array();
        if (!isset($objs[self::$shard_cluster_id])) {
            $obj = new Memcached();
            $shard_master = self::getConfig(sprintf('sharding.masters.%d', self::$shard_cluster_id));
            if (!is_array($shard_master)) {
                throw new Exception();
            }
            $obj->addServer($shard_master['memcache_host'], $shard_master['memcache_port']);
            $objs[self::$shard_cluster_id] = $obj;
        }

        self::$objShardingIndexCacher = $objs[self::$shard_cluster_id];
    }

    /**
     * X-Connector
     *
     * @throws Exception
     * @return void
     */
    private function xconnect() {
        $link = $this->_xconnect();
        if ($link === false) {
            throw new Exception();
        }

        if (!$link->select_db($this->dbname)) {
            throw new Exception('select db failed');
        }

        $this->link = $link;
    }

    /**
     *
     * @return mysqli
     */
    private function _xconnect() {
        if (isset(self::$links[$this->link_key])) {
            return self::$links[$this->link_key];
        }

        $link = new mysqli();
        if (!$link->real_connect($this->host, $this->username, $this->passwd, $this->dbname, $this->port, null, MYSQLI_CLIENT_COMPRESS)) {
            throw new Exception('db error (%d): %s', $link->connect_errno, $link->connect_error);
        }

        if (!$link->set_charset(self::getConfig('core.charset', 'utf8'))) {
            throw new Exception('db error (%d): %s', $link->errno, $link->error);
        }

        self::$links[$this->link_key] = $link;

        return $link;
    }

    /**
     *
     * @param string $sql
     * @throws Exception
     * @return DB
     */
    public function query($sql = null) {
        if (!isset($sql)) {
            $sql = $this->builder();
        }
        $this->xconnect();
        if (!is_string($sql)) {
            throw new Exception();
        }
        $result = $this->link->query($sql);
        if ($result === false) {
            throw new Exception('failed to query on db');
        }
        $this->sql = $sql;
        $this->result = $result;
        return $this;
    }

    /**
     *
     * @throws Exception
     */
    public function beginTransaction() {
        $this->xconnect();
        $this->link->autocommit(false);
    }

    /**
     *
     * @throws Exception
     */
    public function commit() {
        $this->xconnect();
        if (!$this->link->commit()) {
            throw new Exception('failed to commit');
        }
        $this->link->autocommit(true);
    }

    /**
     *
     * @throws Exception
     */
    public function rollBack() {
        $this->xconnect();
        if (!$this->link->rollback()) {
            throw new Exception('failed to rollback');
        }
        $this->link->autocommit(true);
    }

    /**
     *
     * @throws Exception
     * @return integer/string
     * @see http://php.net/mysqli
     */
    public function lastInsertId() {
        $this->xconnect();
        return $this->link->insert_id;
    }

    /**
     *
     * @throws Exception
     * @return array, false
     */
    public function fetch() {
        if ($this->result instanceof mysqli_result) {
            $row = $this->result->fetch_assoc();
            if (!$row) {
                $this->result->free();
                $this->result = null;
            }
            return $row;
        } else {
            throw new Exception('result is not a mysqli_result');
        }
    }

    /**
     *
     * @throws Exception
     * @return array
     */
    public function fetchAll() {
        if (!($this->result instanceof mysqli_result)) {
            throw new Exception('result is not a mysqli_result');
        }
        $rows = array();
        while ($row = $this->result->fetch_assoc()) {
            $rows[] = $row;
        }
        $this->result->free();
        $this->result = null;
        return $rows;
    }

    /**
     *
     * @param mixed $value, can be int/float/string
     * @throws Exception
     * @return string
     */
    public function quote($value) {
        if (is_bool($value)) {
            return $value ? '1' : '0';
        } else if (is_int($value)) {
            return (string)$value;
        } else if (is_float($value)) {
            return sprintf('%F', $value);
        } else if (is_string($value)) {
            return "'" . $this->escape($value) . "'";
        } else {
            throw new Exception();
        }
    }

    /**
     *
     * @param string $value
     * @throws Exception
     * @return string
     */
    private function escape($value) {
        if (!is_string($value)) {
            throw new Exception();
        }

        $this->xconnect();

        return mysqli_real_escape_string($this->link, $value);
    }

    /**
     *
     * @param array $dsn
     * @return array, false on failure
     */
    private static function parseDSN($dsn) {
        $infos = parse_url($dsn);
        if ($infos === false) {
            return false;
        }

        return array(
            'host' => $infos['host'],
            'port' => isset($infos['port']) ? $infos['port'] : 3306,
            'username' => $infos['user'],
            'passwd' => $infos['pass'],
            'dbname' => substr($infos['path'], 1)
        );
    }

    /**
     *
     * @param array $config
     * @throws Exception
     */
    public static function setConfig($config) {
        if (!is_array($config)) {
            // TODO more check code?
            throw new Exception();
        }
        self::$config = $config;
    }

    /**
     *
     * @param string $name
     * @param string | null $default, optional, defaults to null
     */
    private static function getConfig($name, $default = NULL) {
        if (!is_string($name)) {
            throw new Exception();
        }
        $sections = explode('.', $name);
        $config = self::$config;
        while ($section = array_shift($sections)) {
            if (isset($config[$section])) {
                $config = $config[$section];
            } else {
                return $default;
            }
        }
        return $config;
    }

    /**
     *
     * @param integer $shard_key
     * @throws Exception
     * @return string
     */
    private static function getIndexCacheKey($shard_key) {
        if (!is_int($shard_key)) {
            throw new Exception();
        }
        return sprintf('db/sharding/%d', self::$shard_cluster_id, $shard_key);
    }
}