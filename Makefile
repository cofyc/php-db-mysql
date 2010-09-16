
tests:
	phpunit --bootstrap tests/bootstrap.php tests

docs:
	phpdoc -o HTML:frames:earthli -f src/DB.php -t docs

.PHONY: tests docs
