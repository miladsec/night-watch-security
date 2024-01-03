import argparse


def add_args(parser):
    parser.add_argument('--version', required=False, help='Show the current version')


def main():
    parser = argparse.ArgumentParser()
    add_args(parser)
    args = parser.parse_args()

    if args.version:
        # call_method(args.version)
        return


if __name__ == '__main__':
    main()
