<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

class DB {

    /**
     *
     * @var DB
     */
    private static $instance;

    /**
     * Shard DBs
     * @var DB
     */
    private static $instances = array();

    /**
     *
     * @var array
     */
    private static $config;

    /**
     * Currently selected
     *
     * @var mysqli
     */
    private $link;

    /**
     * Link pool
     *
     * @var array, array of mysqlis
     */
    private static $links;

    /**
     * Master Switch
     *
     * @var boolean
     */
    private $master = false;

    /**
     *
     * @var string
     */
    private $dsn;

    /**
     *
     * @var
     */
    private $host;

    private $port;

    private $username;

    private $passwd;

    private $dbname;

    /**
     *
     * @var array
     */
    private $dsn_slaves;

    /**
     *
     * @var Memcached
     */
    private static $objMemcached;

    /**
     *
     * @var mysqli
     */
    private static $objLinkIndex;

    /**
     *
     * @var array
     */
    private static $shard_indices = array();

    /**
     *
     * @param integer $shard_key, optional
     */
    private function __construct($shard_key = null) {
        if (!isset($shard_key)) {
            $this->dsn = self::getConfig('global.master');
            return $this;
        }

        // sharding
        try {
            $shard = self::sharding($shard_key);
        } catch (Exception $e) {
            throw new $e();
        }

        $this->dsn = $shard['master'];
    }

    /**
     *
     * @param integer $shard_key
     * @throws Exception
     * @return array $shard
     */
    private static function sharding($shard_key) {
        if (!is_int($shard_key)) {
            throw new Exception();
        }

        $shards = self::getConfig('shards');
        if (!is_array($shards)) {
            throw new Exception();
        }

        if (!isset(self::$shard_indices[$shard_key])) {
            if (!isset(self::$objMemcached)) {
                self::$objMemcached = new Memcached();
                if (!self::$objMemcached->addServer(self::getConfig('core.memcache_host'), self::getConfig('core.memcache_port'))) {
                    throw new Exception();
                }
            }

            $shard_id = self::$objMemcached->get($shard_key);
            if ($shard_id === false) {
                $infos = self::parseDSN(self::getConfig('core.master'));
                if (!$infos) {
                    throw new Exception();
                }
                if (!isset(self::$objLinkIndex)) {
                    self::$objLinkIndex = self::_xconnect($infos['host'], $infos['port'], $infos['username'], $infos['passwd'], $infos['dbname']);
                }
                if (!self::$objLinkIndex->select_db($infos['dbname'])) {
                    throw new Exception();
                }

                try {
                    // read from index db
                    $result = self::$objLinkIndex->query("SELECT shard_id FROM index_user WHERE uid = " . (int)$shard_key);
                    if (!$result) {
                        throw new Exception();
                    }
                    $row = $result->fetch_assoc();
                    if (!$row || !isset($row['shard_id'])) {
                        throw new Exception();
                    }
                    $shard_id = (int)$row['shard_id'];
                } catch (Exception $e) {
                    // TODO random allocate
                    $shard_id = self::random_sharding($shards);
                }

                if (!isset($shards[$shard_id])) {
                    throw new Exception('shard id does not exist');
                }

                $sql = 'INSERT INTO index_user
                	( `uid`
                	, `shard_id`
                	) VALUE
                	( ' . (int)$shard_key . '
                	, ' . (int)$shard_id . '
                	)
				';
                if (!self::$objLinkIndex->query($sql)) {
                    throw new Exception();
                }

                @self::$objMemcached->set($shard_key, $shard_id); // ignore this error
            }

            self::$shard_indices[$shard_key] = $shard_id;
        }

        if (!isset($shards[self::$shard_indices[$shard_key]])) {
            throw new Exception();
        }

        return $shards[self::$shard_indices[$shard_key]];
    }

    /**
     *
     * @param array $shards
     * @throws Exception
     * @return integer
     */
    private static function random_sharding($shards) {
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
     * @param integer $shard_key, optional
     * @return DB
     */
    public static function getInstance($shard_key = null) {
        if (!isset($shard_key)) {
            if (!isset(self::$instance)) {
                self::$instance = new self();
            }
            return self::$instance;
        }

        if (!isset(self::$instances[$shard_key])) {
            self::$instances[$shard_key] = new self($shard_key);
        }
        return self::$instances[$shard_key];
    }

    /**
     *
     * @param string $config_file
     */
    public static function loadConfigFromFile($config_file) {
        $config = @parse_ini_file($config_file, true);
        if ($config === false) {
            throw new Exception();
        }
        foreach ($config as $key => $val) {
            if (strpos($key, 'shard') === 0) {
                $pieces = explode(' ', $key);
                if (count($pieces) === 2 && $pieces[0] === 'shard') {
                    if (!ctype_digit($pieces[1]) || $pieces[1] <= 0) {
                        throw new Exception('shard id should be positive number');
                    }
                    self::$config['shards'][$pieces[1]] = $val;
                }
            } else if ($key === 'core') {
                self::$config['core'] = $val;
            } else if ($key === 'global') {
                self::$config['global'] = $val;
            } else {
                // ignore
            }
        }
    }

    /**
     *
     * @param string $configname
     * @param string | null $default, optional, defaults to null
     */
    private static function getConfig($configname, $default = NULL) {
        if (!is_string($configname)) {
            throw new Exception();
        }
        $sections = explode('.', $configname);
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
     * @param string $sql, optinal
     * @throws Exception, if we cannot connect to db
     * @return void
     */
    private function xconnect($sql) {
        $infos = self::parseDSN($this->dsn);
        if (!$infos) {
            throw new Exception('dsn format is wrong');
        }

        $this->host = $infos['host'];
        $this->port = $infos['port'];
        $this->username = $infos['username'];
        $this->passwd = $infos['passwd'];
        $this->dbname = $infos['dbname'];

        //        if (self::isReadSql($sql) && !$this->master) {
        //            $dsn_slaves = self::getConfig('global.slaves');
        //            if (isset($dsn_slaves)) {
        //                $dsn = $dsn_slaves[array_rand($dsn_slaves)];
        //            }
        //        }


        $link = self::_xconnect($this->host, $this->port, $this->username, $this->passwd, $this->dbname);
        if ($link === false) {
            throw new Exception();
        }

        $this->link = $link;
    }

    /**
     * DB connector with a connection pool.
     *
     * @param string $host
     * @param integer $port
     * @param string $username
     * @param string $passwd
     * @param string $dbname
     * @return mysqli, false on failure
     */
    private static function _xconnect($host, $port, $username, $passwd, $dbname) {
        $mysql_unique_id = $host . $port;

        if (isset(self::$links[$mysql_unique_id])) {
            return self::$links[$mysql_unique_id];
        }

        $link = new mysqli();
        if (!$link->real_connect($host, $username, $passwd, $dbname, $port, null, MYSQLI_CLIENT_COMPRESS)) {
            return false;
        }

        if (!$link->set_charset(self::getConfig('core.charset', 'utf8'))) { // TODO 错误处理
            return false;
        }

        self::$links[$mysql_unique_id] = $link;

        return $link;
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
     * @param string $sql
     * @return mysqli_result or true, false on failure
     */
    public function query($sql) {
        $this->xconnect($sql);
        if (!$this->link->select_db($this->dbname)) {
            return false;
        }
        return $this->link->query($sql);
    }

    /**
     *
     * @param mixed $value, can be int/float/string
     * @return string
     */
    public function quote($value) {
        if (is_int($value)) {
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
     * @param boolean $master
     * @return
     */
    public function setMaster($master) {
        if (!is_bool($master)) {
            throw new Exception();
        }

        $this->master = $master;
    }

    /**
     *
     * @param string $value
     * @return string
     */
    private function escape($value) {
        if (!is_string($value)) {
            throw new Exception();
        }

        // connect to slave it presents with this sql
        $this->xconnect('show status');

        return mysqli_real_escape_string($this->link, $value);
    }

    /**
     *
     * @param string $sql
     * @return boolean
     */
    private static function isReadSql($sql) {
        static $r_ops = array(
            'select',
            'show',
            'desc'
        );
        $sql = strtolower(trim($sql));
        foreach ($r_ops as $op) {
            if (strpos($sql, $op) === 0) return true;
        }
        return false;
    }
}