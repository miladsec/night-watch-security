import argparse

from src.helpers.base import read_yaml_config


def add_args(parser):
    parser.add_argument('--version', action='store_true', help='Show the current version')


def main():
    parser = argparse.ArgumentParser()
    add_args(parser)
    args = parser.parse_args()

    base_configs = read_yaml_config()

    if args.version:
        print(f"you are using {base_configs.get('nws', {}).get('version')} version.")


if __name__ == '__main__':
    main()
