.PHONY: snapshots
snapshots:
	clear
	spark-submit --master local --deploy-mode client main.py > output_snapshots.txt

.PHONY: black
black:
	black -l 78 .

.PHONY: git
git:
	git add . && git commit -m "Some work lol" && git push origin main

.PHONY: agg
agg:
	spark-submit --master local --deploy-mode client aggregations.py > output_agg.txt
