
mysql:
	phpunit --bootstrap tests/bootstrap.php tests/MySQLTest.php

redis:
	phpunit --bootstrap tests/bootstrap.php tests/RedisTest.php

.PHONY: mysql redis