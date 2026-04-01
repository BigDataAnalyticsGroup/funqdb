# 06 FQL Operators

OLD version: currently being refactored and split up into separate files

AF: attribute function

## Filters

| signature       | parameters                         | input function | output function                           | semantic                                                                                                    | example                                                           |
|:----------------|:-----------------------------------|:---------------|:------------------------------------------|-------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| DONE `filter_items`  | predicate (phrased against items)  | any AF         | new instance, same type as input function | returns a new instance of the input function containing only the items qualifying under the given predicate | filter relation function on conjunction of its key and its value  |
| DONE `filter_values` | predicate (phrased against values) | any AF         | new instance, same type as input function | same as `filter_items`, but predicate phrased against values                                                | filter values of a relation function                              |
| DONE `filter_keys`   | predicate (phrased against keys)   | any AF         | new instance, same type as input function | same as `filter_items`, but predicate phrased against keys                                                  | filter keys (i.e. relation function names) of a database function |

## Transforms

| signature         | parameters                                                                                             | input function | output function                                    | semantic                                                                                                                                  | example                                                                                                     |
|:------------------|:-------------------------------------------------------------------------------------------------------|:---------------|:---------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| `transform`       | transformation_function (phrased against items)                                                        | any AF         | new instance as created by transformation_function | returns a new transformed instance, similar to existing `map()` in Python, but a valid FQL operator which may be pushed down to an engine | project input AF, return a computed AF based on input AF, project                                           |
| `transform_items` | transformation_function (phrased against items), output_factory (for the attribute function to return) | any AF         | new instance as created by the output factory      | returns a new instance of the input function containing the transformed items of the input AF                                             | aggregate multiple items into a single item (like done in classical aggregation), project items of input AF |

## Grouping and Partitioning

| signature            | parameters                                                                                             | input function | output function                               | semantic                                                                            | example                                                                                                                           |
|:---------------------|:-------------------------------------------------------------------------------------------------------|:---------------|:----------------------------------------------|-------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| DONE `partition`     | partitioning_function (phrased against items), output_factory (for the attribute function to return)   | any AF         | new instance as created by the output factory | returns a new instance of the input function containing the partitions              | horizontally partition (aka grouping) or vertically partition the input AF, replicate the input AF, any combination of the latter |
| DONE `group_by`           | partitioning keys (phrased against items), output_factory (for the attribute function to return)       | any AF         | new instance as created by the output factory | returns a new instance of the input function containing the partitions              | horizontally partition (aka grouping) or vertically partition the input AF, replicate the input AF, any combination of the latter |
| `aggregate`          | aggregate input AF based on the specified aggregation functions                                        | any AF         | same instance type as input AF                | returns a new instance of the input function containing the aggregation results     | classical aggregation without grouping but including schema definition of the created AF                                          |
| `group_by_aggregate` | partitions input AF based on a lambda function, then aggregates each input partition into an output AF | any AF         | new instance as created by the output factory | returns a new instance of the input function containing the partitions (aka groups) | classical group_by_aggregate (which in SQL is three operators: grouping, aggregation, projection)                                 |
| `grouping_sets`      | partition on multiple conditions at the same time                                                      | any AF         | new instance as created by the output factory | returns a new instance of the input function containing the partitions (aka groups) | classical grouping sets however not shoehorned into a single output relation                                                      |
| `cube`               | partition on all conditions in a cube at the same time                                                 | any AF         | new instance as created by the output factory | returns a new instance of the input function containing the partitions (aka groups) | classical cube however not shoehorned into a single output relation                                                               |

## Subdatabase Operators

| signature      | parameters | input function | output function | semantic                                       | example                                                                                                    |
|:---------------|:-----------|:---------------|:----------------|------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `subdatabase`  | TODO       | DBF            | DBF             | returns a DBF reduced to the participating TFs | classical Yannakakis-reduction, resultdb                                                                   |
| `grouping_set` | TODO       | DBF            | DBF             | returns a DBF with grouping sets               | classical grouping sets, but not hacked into one output relation as in SQL                                 |
| `cube`         | TODO       | DBF            | DBF             | returns a DBF with cube results                | classical cube, but not hacked into one output relation as in SQL                                          |
| `outer`        | TODO       | DBF            | DBF             | returns a DBF with additional outer partitions | subdatabase plus outer partitions for the RFs specified, but not hacked into one output relation as in SQL |
| `anti`         | TODO       | DBF            | DBF             | returns a DBF with additional anti partitions  | subdatabase plus anti partitions for the RFs specified, but not hacked into one output relation as in SQL  |

## (Flattening) Joins

| signature                       | parameters | input function | output function | semantic                                                    | example               |
|:--------------------------------|:-----------|:---------------|:----------------|-------------------------------------------------------------|-----------------------|
| `join`                          | TODO       | DBF            | RF              | returns a join RF of the input DBF                          | classical join        |
| `equi_join`                     | TODO       | DBF            | RF              | returns a join RF of the input DBF                          | classical join        |
| `left, right , full outer join` | TODO       | DBF            | DBF             | returns inner join plus outer results in separate relations | classical outer joins |

TODO, ongoing...