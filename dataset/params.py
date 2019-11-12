import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        argument_default=argparse.SUPPRESS
    )
    parser.add_argument(
        '--local', dest='local', action='store_true',
        help='Running the code local', default=False
    )

    return parser.parse_args()
