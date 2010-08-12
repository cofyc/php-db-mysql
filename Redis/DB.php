<?php
/**
 *
 *
 * @link http://github.com/owlient/phpredis
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

class DB {

    /**
     * for global instance
     * @var DB
     */
    private static $instance;

    /**
     * for sharding instances
     */
    private static $instances = array();

    /**
     * Redis For Sharding
     * @var Redis
     */
    private static $objRedisMaster;

    /**
     *
     * @var array
     */
    private static $config;

    /**
     *
     * @var Redis
     */
    private $objRedis;

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
     * @param integer $shard_key, optional
     * @return DB
     */
    private function __construct($shard_key = null) {
        if (!isset($shard_key)) {
            list($this->host, $this->port) = self::parseDSN(self::getConfig('global.master'));
        }

        // sharding
        if (is_int($shard_key)) {
            throw new Exception();
        }

        if (!isset(self::$objRedisMaster)) {
            self::$objRedisMaster = new Redis();
            list($host, $port) = self::parseDSN(self::getConfig('core.master'));
            if (!self::$objRedisMaster->connect($host, $port)) {
                throw new Exception('cannot connect to master redis server');
            }
        }

    //
    }

    /**
     *
     * @param integer $shard_key, optional
     * @return DB
     */
    public static function getInstance($shard_key = null) {
        if (isset($shard_key)) {
            if (!isset(self::$instances[$shard_key])) {
                self::$instances[$shard_key] = new self();
            }
            return self::$instances[$shard_key];
        } else {
            if (!isset(self::$instance)) {
                self::$instance = new self();
            }
            return self::$instance;
        }

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
     * @param string $config
     * @return array
     * - string (host)
     * - integer (port)
     */
    private static function parseDSN($config) {
        if (!is_string($config)) {
            throw new Exception();
        }
        $configs = explode(':', $config);
        if (!is_array($configs) || count($configs) !== 2) {
            throw new Exception();
        }
        return array(
            $configs[0], // host
            (int)$configs[1] // port
        );
    }
}