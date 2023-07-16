.PHONY: run
run:
	clear
	spark-submit --master local --deploy-mode client main.py

.PHONY: black
black:
	black -l 78 .

.PHONY: git
git:
	git add . && git commit -m "Some work lol" && git push origin main