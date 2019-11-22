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
    parser.add_argument(
        '--f', dest='female', action='store_true',
        help='Finds the most common adjectives for the female wikipedia', default=False
    )
    parser.add_argument(
        '--m', dest='male', action='store_true',
        help='Finds the most common adjectives for the male wikipedia', default=False
    )

    return parser.parse_args()
