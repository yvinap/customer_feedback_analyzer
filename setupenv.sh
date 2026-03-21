#!/usr/bin/env bash
# =============================================================================
# setupenv.sh — Bootstrap the Customer Feedback Analyzer development environment
# =============================================================================
# Usage:
#   bash setupenv.sh          # full setup
#   bash setupenv.sh --cdk    # also bootstrap CDK for your AWS account/region
# =============================================================================
set -euo pipefail

PYTHON_BIN="python3.11"
VENV_DIR=".venv"
CDK_DIR="cdk"
AWS_PROFILE="${AWS_PROFILE:-aws_swami}"

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║        Customer Feedback Analyzer — Environment Setup        ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Validate Python 3.11 ─────────────────────────────────────────────────────
info "Checking for Python 3.11 ..."
if ! command -v "$PYTHON_BIN" &>/dev/null; then
    # Fall back to python3 and verify version
    if python3 --version 2>&1 | grep -q "Python 3\.11"; then
        PYTHON_BIN="python3"
    else
        error "Python 3.11 not found. Install it via 'brew install python@3.11' or from python.org."
    fi
fi
PY_VERSION=$("$PYTHON_BIN" --version 2>&1)
success "Found $PY_VERSION"

# ── Create virtual environment ────────────────────────────────────────────────
if [ ! -d "$VENV_DIR" ]; then
    info "Creating virtual environment in ./$VENV_DIR ..."
    "$PYTHON_BIN" -m venv "$VENV_DIR"
    success "Virtual environment created."
else
    info "Virtual environment already exists at ./$VENV_DIR — skipping creation."
fi

# ── Activate venv ─────────────────────────────────────────────────────────────
# shellcheck disable=SC1090
source "$VENV_DIR/bin/activate"
success "Virtual environment activated."

# ── Upgrade pip ───────────────────────────────────────────────────────────────
info "Upgrading pip, setuptools, wheel ..."
pip install --quiet --upgrade pip setuptools wheel
success "pip upgraded to $(pip --version | awk '{print $2}')"

# ── Install project dependencies ─────────────────────────────────────────────
info "Installing project dependencies from requirements.txt ..."
pip install --quiet -r requirements.txt
success "Project dependencies installed."

# ── Install CDK dependencies ──────────────────────────────────────────────────
if [ -f "$CDK_DIR/requirements.txt" ]; then
    info "Installing CDK dependencies from $CDK_DIR/requirements.txt ..."
    pip install --quiet -r "$CDK_DIR/requirements.txt"
    success "CDK dependencies installed."
fi

# ── Install Node.js / AWS CDK CLI ─────────────────────────────────────────────
info "Checking for AWS CDK CLI (npm package) ..."
if ! command -v cdk &>/dev/null; then
    if command -v npm &>/dev/null; then
        info "Installing AWS CDK CLI globally via npm ..."
        npm install -g aws-cdk
        success "AWS CDK CLI installed: $(cdk --version)"
    else
        warn "npm not found. Install Node.js ≥ 18 and run 'npm install -g aws-cdk' manually."
    fi
else
    success "AWS CDK CLI already available: $(cdk --version)"
fi

# ── Validate AWS credentials ──────────────────────────────────────────────────
info "Validating AWS credentials for profile '$AWS_PROFILE' ..."
if AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity --output table 2>/dev/null; then
    success "AWS credentials are valid."
else
    warn "Could not validate AWS credentials for profile '$AWS_PROFILE'."
    warn "Ensure ~/.aws/credentials contains the 'aws_swami' profile."
fi

# ── Optional CDK bootstrap ────────────────────────────────────────────────────
if [[ "${1:-}" == "--cdk" ]]; then
    info "Bootstrapping CDK environment (this may take a few minutes) ..."
    AWS_REGION=$(AWS_PROFILE="$AWS_PROFILE" aws configure get region --profile "$AWS_PROFILE" 2>/dev/null || echo "us-east-1")
    (
        cd "$CDK_DIR"
        AWS_PROFILE="$AWS_PROFILE" cdk bootstrap "aws://$(AWS_PROFILE=$AWS_PROFILE aws sts get-caller-identity --query Account --output text)/$AWS_REGION"
    )
    success "CDK bootstrap complete."
fi

# ── Print next steps ──────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    Setup complete! 🎉                        ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Activate the virtual environment:"
echo "    source .venv/bin/activate"
echo ""
echo "  Upload sample data to S3:"
echo "    python scripts/upload_sample_data.py"
echo ""
echo "  Deploy infrastructure (from cdk/ directory):"
echo "    cd cdk && AWS_PROFILE=aws_swami cdk deploy --all"
echo ""
echo "  Trigger a pipeline run:"
echo "    python scripts/trigger_pipeline.py --data-type text_reviews"
echo ""
echo "  Re-run with CDK bootstrap:"
echo "    bash setupenv.sh --cdk"
echo ""
