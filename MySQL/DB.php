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
    private $links;

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
     * @param integer $shard_key, optional
     */
    private function __construct($shard_key = null) {
        if (!isset($shard_key)) {
            $this->dsn = self::getConfig('global.master');
        }

        // sharding
        if (is_int($shard_key)) {
            throw new Exception();
        }

        if (!isset(self::$objMemcached)) {
            self::$objMemcached = new Memcached();
            self::$objMemcached->addServer(self::getConfig('core.memcache_host'), self::getConfig('core.memcache_port'));
        }
    }

    /**
     *
     * @param integer $shard_key, optional
     * @return DB
     */
    public static function getInstance($shard_key = null) {
        if (!isset(self::$instance)) {
            self::$instance = new self($shard_key);
        }
        return self::$instance;
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
            // sections starting with 'shard' all are shard slots
            if (strpos($key, 'shard') === 0) {
                self::$config['shards'][] = $val;
            } else {
                self::$config[$key] = $val;
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
        $dsn = $this->dsn;

        if (self::isReadSql($sql) && !$this->master) {
            $dsn_slaves = self::getConfig('global.slaves');
            if (isset($dsn_slaves)) {
                $dsn = $dsn_slaves[array_rand($dsn_slaves)];
            }
        }

        $link = $this->_xconnect($dsn);
        if ($link === false) {
            throw new Exception();
        }

        $this->link = $link;
    }

    /**
     *
     * @param string $dsn
     * @return mysqli, false on failure
     */
    private function _xconnect($dsn) {
        $infos = parse_url($dsn);
        if ($infos === false) {
            throw new Exception();
        }

        $host = $infos['host'];
        $port = isset($infos['port']) ? $infos['port'] : 3306;
        $username = $infos['user'];
        $passwd = $infos['pass'];
        $dbname = substr($infos['path'], 1);
        $mysql_unique_id = $host . $port;

        if (isset($this->links[$mysql_unique_id])) {
            return $this->links[$mysql_unique_id];
        }

        $link = new mysqli();
        if (!$link->real_connect($host, $username, $passwd, $dbname, $port, null, MYSQLI_CLIENT_COMPRESS)) {
            return false;
        }

        if (!$link->set_charset(self::getConfig('core.charset', 'utf8'))) { // TODO 错误处理
            return false;
        }

        $this->links[$mysql_unique_id] = $link;

        return $link;
    }

    /**
     *
     * @param string $sql
     * @return mysqli_result or true, false on failure
     */
    public function query($sql) {
        $this->xconnect($sql);
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

        $this->xconnect();

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