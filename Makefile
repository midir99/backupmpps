clean:
	find . -name '__pycache__' | xargs rm -rf


format:
	( \
	. .venv/bin/activate; \
	python3 -m black backupmpps.py; \
	python3 -m isort backupmpps.py; \
	)


lint:
	( \
	. .venv/bin/activate; \
	python3 -m pylint backupmpps.py; \
	)

venv:
	( \
	python3 -m venv .venv; \
	. .venv/bin/activate; \
	python3 -m pip install -r requirements.txt; \
	)


venv-dev:
	( \
	python3 -m venv .venv; \
	. .venv/bin/activate; \
	python3 -m pip install -r requirements.txt; \
	python3 -m pip install -r requirements.dev.txt; \
	)
