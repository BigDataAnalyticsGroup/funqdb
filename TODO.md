### To Do List

- [ ] relationship functions
- [ ] n:m relationships
- [ ] add support for composite primary keys
- [ ] operator: output a plan, how? as everything is functions and the input to an operator is not another operator
  -> explain must traverse through the call chain including attribute functions!
- [ ] improve documentation and add more examples
- [ ] implement more unit tests for edge cases

### Discarded

- [ ] ~~get rid of `__call__` in operators and integrate into `__init__`~~
  No, actually good to keep it apart. Other option: make these classes functions instead of classes. Everything in one
  call.

