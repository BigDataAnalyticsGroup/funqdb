### To Do List

- [ ] relationship functions
- [ ] n:m relationships
- [ ] add support for composite primary keys
- [ ] operator: output a plan, how?
  - [ ] as everything is functions and the input to an operator is not another operator
    -> explain must traverse through the call chain including attribute functions!
  - [ ] maybe through a tainting mechanism, i.e. make the AF being passed through and let it collect information on the way
    through the pipeline!
- [ ] improve documentation and add more examples
- [ ] implement more unit tests for edge cases
- [ ] maybe:
  1. the first get to an item in the AE triggers the actual computation
  2. get the lineage(aka the logical plan of the AE
  3. ...
  4. that determines at the same time the root of the computation

### Discarded

- [ ] ~~get rid of `__call__` in operators and integrate into `__init__`~~
  No, actually good to keep it apart. Other option: make these classes functions instead of classes. Everything in one
  call.

