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
     * @param string $sql, optinal
     * @return void
     */
    private function xconnect($sql = null) {
        if (isset($sql)) {
            if (self::isReadSql($sql) || !$this->master) {

            }
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
    	if (!$link->real_connect($host, $username, $passwd, $dbname, null, null, MYSQLI_CLIENT_COMPRESS)) {
    		// TODO 错误处理
    	}

    	if (!$link->set_charset('UTF-8')) {
    		// TODO 错误处理
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