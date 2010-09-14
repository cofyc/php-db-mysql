<?php
/**
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

$cluster_id = 0;
$shard_from_id = 0;
$shard_to_id = 0;

$opts = getopt(null, array(
    "cluster:",
    "from:",
    "to:",
));

if (!$opts) {
    usage();
    exit(0);
}

foreach ($opts as $opt => $val) {
    switch ($opt) {
        case 'cluster':
            check_digit($opt, $val);
            $cluster_id = intval($val);
            break;
        case 'from':
            check_digit($opt, $val);
            $shard_from_id = intval($val);
            break;
        case 'to':
            check_digit($opt, $val);
            $shard_to_id = intval($val);
            break;
        default:
            printf("Unknown option: %s.\n", $opt);
            break;
    }
}

if (posix_isatty(STDIN)) {
    printf("Transfer user data from %d to %d on cluster %d by entering user id (use Ctrl-D to exit):\n", $shard_from_id, $shard_to_id, $cluster_id);
}

while (true) {
    $line = fgets(STDIN);
    if ($line === false) {
        // EOF
        exit(0);
    }
    $line = trim($line);

    if (!ctype_digit($line)) {
        printf(">>> Error, not a number.\n");
        continue;
    }
    $userid = intval($line);
    if ($userid <= 0) {
        printf(">>> Error, user should be greater than zero.\n");
        continue;
    }

    printf(">>> Transferring userid %d...", $userid);

    try {
//        dbcluster_transfer($cluster_id, $shard_from_id, $shard_to_id, $userid);
        printf(" ok.\n");
    } catch (Exception $e) {
        printf(" failed, error: %s (code: %d, line: %d).\n", $e->getMessage(), $e->getCode(), $e->getLine());
    }
}

/**
 *
 * @return void
 */
function usage() {
    printf("Usage: php -f %s -- --cluster <cluster_id> --from <shard_id> --to <shard_id>\n", basename(__FILE__));
}

/**
 *
 * @param string $opt
 * @param string $val
 * @return void
 */
function check_digit($opt, $val) {
    if (!ctype_digit($val)) {
        die(sprintf("%s should be number, '%s' provided.", $opt, $val));
    }
}
