# Set-up a repository from this template

## Set Repository Name

- Clone this repository and run the following command

    ```shell
    $ ./setup.sh -n <repository name on GitHub>
    $ ./setup.sh -n necst-lib
    ```

## `LICENSE`

- Fix copyright year (`YYYY` or `YYYY - YYYY` format)

## `README.md`

- Edit package description accordingly

## `pyproject.toml`

- Replace `TEMPLATE` description, with a text written in repository `About` field on GitHub
- Fix authors

## `tests/test_nothing.py`

- Remove or rename.

## Install current project

- Run the following command at repository root

    ```shell
    $ poetry install
    ```

## `setup` directory

- Remove

## Upload configured files

- Run the following command at repository root

    ```shell
    $ git add .
    $ git commit -m "Set-up the repository"
    $ git push
    ```
