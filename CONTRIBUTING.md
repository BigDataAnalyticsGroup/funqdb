# Contributing

## Financial contributions

Eventually, I would like to back the development of this project by more people, ideally as part of a foundation.
If you are interested in supporting this project through such a foundation financially, get in touch.

If you are a developer and want to contribute code, in general, for bug reports and small fixes, you can just open
an issue or a PR. For larger features, we first discuss the feature in an issue before starting to implement it, to make
sure that we are on the same page about the feature and its implementation.

## Code contributions

For any PR make sure:

1. that your PR is about a single issue, e.g. a bug fix, a new feature, etc., not a mix of multiple issues,
2. if in doubt, ask back, i.e. if your PR gets too big, or if you are not sure about the design, APIs, implementation,
   ask back early to make sure that we are on the same page
3. that all your code is unit tested, you should have tests for all new features and bug fixes
4. that you use typehints wherever possible, function signatures, variable assignments, etc.
5. that you test (line) coverage is high, ideally 100%, but at least above 90%
6. that all tests pass before submitting your PR, and that you have run all tests locally before submitting your PR
7. that your code is well documented:
    - there are docstrings for all public functions and classes
    - there are meaningful code comments for all non-trivial code
    - all conflicts with typehints are resolved (e.g. marked orange in PyCharm)
    - the documentation was updated if you add new features or change existing ones, or change APIs
    - (optional) the tutorial was updated if you add new features or change existing ones, or change APIs
