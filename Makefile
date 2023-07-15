.PHONY: run
run:
	clear
	spark-submit --master local --deploy-mode client main.py

.PHONY: black
black:
	black -l 78 .