
### Accessing Data

To access data, we can simply call the functions with the appropriate keys. For example, to retrieve the name of the
person associated with key "t1" in relation *persons*, we can simply use a dot syntax:

```python
name: str = db.persons.t1.name 
```

Or, if we define `persons` as a variable:

```python
persons: RF = db.persons  # This will return the relation function representing the set of persons
```

Then, we can write:

```python
name: str = persons.t1.name 
```

Alternatively, we may use the [ ]-syntax:

```python   
name: str = persons["t1"]["name"] 
```

Or the ( )-syntax:

```python
name: str = persons("t1")("name") 
```

Or any mix you like (not recommended):

```python
name: str = persons("t1").name 
```

All of these statements are equivalent and will yield the same result, i.e. the string "Tom".

Note, that in Python, the dot syntax is only available for attributes that are valid **Python identifiers**, e.g. the
`name`
is
valid, but the integer `1` is not, so you cannot do `persons.t1.1`, but you can do `persons.t1[1]` or
`persons("t1")(1)`.

In general, I would recommend to stick to the dot syntax for attributes that are valid identifiers, and use the []
-syntax for all other cases. In other words, the dot syntax is more concise and easier to read, but the []-syntax is
more flexible and can handle all cases.

### Items

TODO

***
