pip-install:
	pip install --upgrade pip
	pip install -r requirements.txt

# setup
setup: pip-install
	@echo "setup complete."

.PHONY: pip-install
