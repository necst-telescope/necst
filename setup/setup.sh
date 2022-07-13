#!/bin/bash

# Script for setting up the Python template repository.

# Exit code:
# 0: Success
# 1: Argument error

usage="./$(basename "$0") [-h] [-n str] --Set-up Python template repository.\n\n
where:\n
\t -h  Show this help.\n
\t -n  Repository name on GitHub, case sensitive.
"

# Parse options.
while getopts 'hn:' option
do
    case "${option}" in
        h)  echo -e ${usage}
            exit 0;;
        n) REPOSITORY_NAME=$OPTARG
            ;;
        *)  echo "Invalid argument; see usage below."
            echo -e ${usage}
            exit 1;;
    esac
done

# Check if repository name is specified.
if [ -z "$REPOSITORY_NAME" ]
then
    echo -e "\033[41;1mSpecify repository name.\033[0m\n"
    echo -e ${usage}
    exit 1
fi

# Define REPOSITORY_NAME variants.
PROJECT_NAME=$(echo "$REPOSITORY_NAME" | tr '[:upper:]' '[:lower:]')
PACKAGE_NAME=$(echo "$PROJECT_NAME" | sed 's/-/_/g')

# Confirm set-up configuration.
while true
do
    echo -e  "Setting up \033[46;1m'$REPOSITORY_NAME'\033[0m, using project name \033[46;1m'$PROJECT_NAME'\033[0m and package name \033[46;1m'$PACKAGE_NAME'\033[0m."
    read -p "Proceed? (y/n) " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit 0;;
        * ) echo "Please choose either of (y/n).";;
    esac
done

cd $(dirname $0)/../

mv ./package_name ./$PACKAGE_NAME

# Replace matching strings in files.
for file in 'README.md' 'pyproject.toml' 'docs/conf.py' 'docs/index.rst' 'tests/docs/test_build.py' 'tests/test_nothing.py' '.github/workflows/test.yml'
do
    if [ -f $file ]
    then
        sed -i.bak 's/Package-Name/'$REPOSITORY_NAME'/g' $file
        sed -i.bak 's/package-name/'$PROJECT_NAME'/g' $file
        sed -i.bak 's/package_name/'$PACKAGE_NAME'/g' $file
        rm $file.bak
    fi
done

cd -
exit 0
