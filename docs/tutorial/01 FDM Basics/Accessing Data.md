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

Or a Django-ORM-style `__`-syntax (not recommended):

```python
name: str = persons("t1__name") 
name: str = persons["t1__name"] 
name: str = db["persons__t1__name"] 
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

***

### Items

Every attribute function is an iterable that can directly be used in loops:

```python
person: Item
for person in persons:
    print(person.key, person.value)
```

Note that in contrast to Python dictionaries, there is no need to append `.items()` after `persons`.

We use our own type `Item` rather than Python's inbuilt type `ItemsView` as we needed to add some more functionality (
e.g., for automatic reference swizzling in the store). In addition, we wanted to keep the naming convention of a
key/value-pair.

In addition, similar to Python dictionaries, you may also add `.keys()` or `.values()` to just retrieve the keys or
values,
respectively.

```python
person_key: Key
for person_key in persons.keys():
    print(person_key)
```

or:

```python
person_value: Value
for person_value in persons.values():
    print(person_value)
```

Here, `Key` and `Value`ar placeholders for the types of keys and values of the attribute function. 