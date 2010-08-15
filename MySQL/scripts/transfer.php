#!/usr/bin/env php
<?php

define('CO_ROOT_PATH', realpath(dirname(__FILE__) . '/../'));

require_once '../DB.php';

$shortopts = 'f:t:';

$opts = getopt($shortopts);

if (!$opts) {
    printf("usage: %s -f <shard_from> -t <shard_to>\n", basename(__FILE__));
    exit();
}

$uids = array(
    1,
    3846977
);

DB::loadConfigFromFile(CO_ROOT_PATH . '/config.ini');
foreach ($uids as $uid) {
    printf("transfering %d...", $uid);
    try {
        transfer($uid);
        printf(" ok.\n");
    } catch (Exception $e) {
        printf(" failed. (error: %s)\n", $e->getMessage());
    }
}

/**
 *
 *
 * @param integer $uid
 * @throws Exception
 */
function transfer($uid) {

}
