import json
import argparse
import os


def check_coverage(main_coverage_file, mr_coverage_file):
    """Checks whether the coverage report from the current merge request fulfills the coverage requirements given by the
    current main branch

    Parameters
    ----------
    main_coverage_file : str
        The coverage file for the main branch
    mr_coverage_file: str
        The coverage file for the merge request branch

    Returns
    ----------
    True, if the requirements are met, i.e. all new files have a minimum coverage of at least min_coverage and
    existing files are not worse than on main, False if not.
    """

    # Get coverage parameters from environment
    min_coverage_new = int(os.environ.get("MIN_COVERAGE_NEW"))
    min_coverage_old = int(os.environ.get("MIN_COVERAGE_OLD"))
    coverage_epsilon = float(os.environ.get("COVERAGE_EPSILON"))

    # Check whether the report of the main branch exists
    if not os.path.exists(main_coverage_file):
        print("No coverage report for main found.")
        return True  # Allow branches to merge if no main coverage can be found

    # Set to False during the check in case at least one file has a worse coverage than previously
    coverage_fulfilled = True

    # Read json files
    with open(main_coverage_file, "r") as f:
        main_coverage_dict = json.load(f)["files"]
    with open(mr_coverage_file, "r") as f:
        mr_coverage_dict = json.load(f)["files"]

    for file in main_coverage_dict:
        # Check whether file from main still exists
        if file not in mr_coverage_dict:
            continue

        # Check whether new coverage is at least as good as previously (minus epsilon)
        main_summary = main_coverage_dict[file]["summary"]
        mr_summary = mr_coverage_dict[file]["summary"]
        if (
            mr_summary["percent_covered"]
            < main_summary["percent_covered"] - coverage_epsilon
        ) or mr_summary["percent_covered"] < min_coverage_old:
            print(
                "The coverage for the file "
                + file
                + " is lower than previously! The required coverage is "
                + str(
                    max(
                        main_summary["percent_covered"] - coverage_epsilon,
                        min_coverage_old,
                    )
                )
                + " %, whereas the coverage on this merge request is "
                + str(mr_summary["percent_covered"])
                + " %!"
            )
            coverage_fulfilled = False

    for file in mr_coverage_dict:
        # New files need to have at least a coverage of min_coverage
        if (
            file not in main_coverage_dict
            and mr_coverage_dict[file]["summary"]["percent_covered"] < min_coverage_new
        ):
            print(
                "The coverage for the file "
                + str(file)
                + " is too low. You need at least "
                + str(min_coverage_new)
                + " %, whereas the current coverage is only "
                + str(mr_coverage_dict[file]["summary"]["percent_covered"])
                + " %!"
            )
            coverage_fulfilled = False

    return coverage_fulfilled


if __name__ == "__main__":
    # Set up parser
    parser = argparse.ArgumentParser(
        description="Check whether this merge request fulfills the required coverage statistics."
    )
    parser.add_argument(
        "main_file",
        type=str,
        help="The path to the coverage report from main",
    )
    parser.add_argument(
        "mr_file",
        type=str,
        help="The path to the coverage report from the current merge request",
    )

    args = parser.parse_args()

    if not check_coverage(args.main_file, args.mr_file):
        print("There are files that don't meet the coverage requirements!")
        exit(1)
    else:
        print("Coverage requirements fulfilled!")
