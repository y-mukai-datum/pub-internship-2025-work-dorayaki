pip-install:
	pip install --upgrade pip
	pip install -r requirements.txt

generate-keypair:
	./tools/gen_keypair.sh

# setup
setup: pip-install generate-keypair
	@echo "setup complete."

.PHONY: pip-install generate-keypair
