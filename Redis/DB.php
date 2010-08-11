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

}