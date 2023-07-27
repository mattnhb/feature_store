.PHONY: snapshots
snapshots:
	clear
	spark-submit --master local --deploy-mode client main.py > output_snapshots.txt

.PHONY: black
black:
	black .

.PHONY: git
git:
	git add . && git commit -m "Some work lol" && git push origin main

.PHONY: agg
agg:
	spark-submit --master local --deploy-mode client --driver-memory 4g aggregations.py > output_agg.txt

.PHONY: jsonify
jsonify:
	spark-submit --master local --deploy-mode client --driver-memory 4g  jsonify.py > output_js.txt

