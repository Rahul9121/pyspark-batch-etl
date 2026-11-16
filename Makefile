PYTHON ?= python
export PYTHONPATH := .

.PHONY: install data up down seed jdbc test run pipeline dashboard clean ci

install:
	$(PYTHON) -m pip install -r requirements.txt

data:
	$(PYTHON) src/generate_data.py

up:
	docker compose up -d
	$(PYTHON) scripts/wait_for_postgres.py

down:
	docker compose down

seed: data
	$(PYTHON) scripts/seed_postgres.py

jdbc:
	$(PYTHON) scripts/fetch_jdbc.py

test:
	$(PYTHON) -m pytest -q

run: jdbc seed
	$(PYTHON) src/run_pipeline.py

pipeline: run

dashboard:
	@echo "Open dashboard/index.html in a browser (needs dashboard/metrics.json from make run)"

clean:
	$(PYTHON) -c "import shutil,pathlib;p=pathlib.Path('data/output');shutil.rmtree(p,ignore_errors=True)"

ci: install jdbc data
	$(PYTHON) scripts/seed_postgres.py
	$(PYTHON) -m pytest -q
	$(PYTHON) src/run_pipeline.py
