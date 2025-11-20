.PHONY: help format check lint test clean

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
UV := uv
RUFF := $(UV) run ruff

help: ## Show this help message
	@echo "Available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

format: ## Format code using ruff
	$(RUFF) format .

check: ## Run ruff checks and auto-fix issues
	$(RUFF) check --fix .

lint: format check ## Format code and run linting checks (runs format + check)