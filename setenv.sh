#!/bin/bash

# Check if the OS is Windows (using WSL) or Linux
if [[ "$(uname -s)" == *"NT"* ]]; then
  # Windows (using WSL)
  echo "Setting environment variables for Windows (WSL)..."
  
  # Source the environment file
  source env.delta

  # Convert export commands to SET commands for Windows
  sed 's/^export \([^=]*\)="\(.*\)"/set \1=\2/g' env.delta > env.bat
  
  # Run the generated batch script
  cmd.exe /c env.bat
else
  # Linux
  echo "Setting environment variables for Linux..."
  
  # Source the environment file
  source env.delta
fi
