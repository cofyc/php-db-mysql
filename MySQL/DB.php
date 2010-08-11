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
     *
     * @var boolean
     */
    private $master = false;

    private function __construct() {}

    /**
     *
     * @return DB
     */
    public static function getInstance() {
        if (!isset(self::$instance)) {
            self::$instance = new self();
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
     * @return void
     */
    private function xconnect($sql) {
        if (!isset($sql)) {
        }
        if (self::isReadSql($sql) || !$this->master) {

        } else {

        }
    }

    /**
     *
     * @param string $host
     * @param string $username
     * @param string $passwd
     * @param string $dbname
     */
    private function _xconnect($host, $username, $passwd, $dbname) {
        if (isset($this->links[$host])) {
            return $this->links[$host];
        }

        $link = new mysqli();
        if (!$link->real_connect($host, $username, $passwd, $dbname, null, null, MYSQLI_CLIENT_COMPRESS)) { // TODO 错误处理
        }

        if (!$link->set_charset(self::getConfig('mysql.charset', 'UTF-8'))) { // TODO 错误处理
        }

        return $link;
    }

    /**
     *
     * @param string $sql
     * @return mysqli_result or true, false on failure
     */
    public function query($sql) {
        while (true) {
            $this->xconnect($sql);

            if ($sql) {

            }
        }
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
        }
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