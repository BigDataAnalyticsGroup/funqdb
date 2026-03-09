# 06 FQL Operators

AF: attribute function

| signature       | parameters                         | input function | output function                           | semantic                                                                                                    | example                                                           |
|:----------------|:-----------------------------------|:---------------|:------------------------------------------|-------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| `filter_items`  | predicate (phrased against items)  | any AF         | new instance, same type as input function | returns a new instance of the input function containing only the items qualifying under the given predicate | filter relation function on conjunction of its key and its value  |
| `filter_values` | predicate (phrased against values) | any AF             | new instance, same type as input function | same as `filter_items`, but predicate phrased against values                                                | filter values of a relation function                              |
| `filter_keys`   | predicate (phrased against keys)   | any AF             | new instance, same type as input function | same as `filter_items`, but predicate phrased against keys                                                  | filter keys (i.e. relation function names) of a database function |


TODO, ongoing...