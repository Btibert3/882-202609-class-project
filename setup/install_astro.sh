#!/bin/bash
# Install the Astronomer CLI and configure standalone mode.
# Run once in Cloud Shell. Safe to re-run.
#
# Usage:
#   bash setup/install_astro.sh

set -e

echo "Installing Astro CLI to ~/.local/bin..."
mkdir -p ~/.local/bin

# download the latest release binary directly
LATEST=$(curl -sSL https://api.github.com/repos/astronomer/astro-cli/releases/latest \
  | grep '"tag_name"' | sed 's/.*"v\([^"]*\)".*/\1/')

curl -sSL "https://github.com/astronomer/astro-cli/releases/latest/download/astro_${LATEST}_linux_amd64.tar.gz" \
  | tar -xz -C ~/.local/bin astro

# add to PATH if not already there
if ! grep -q '.local/bin' ~/.bashrc; then
  echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
fi
export PATH="$HOME/.local/bin:$PATH"

echo "Setting dev mode to standalone..."
astro config set dev.mode standalone

echo ""
echo "Done. Verify with: astro version"
echo "Then start Airflow from the project root with: astro dev start"
