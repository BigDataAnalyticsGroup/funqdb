## Example Comparison of SQL and FQL for a Join Query

query <a href="https://raw.githubusercontent.com/gregrahn/join-order-benchmark/refs/heads/master/10c.sql">10 c</a>
<table>
<tr>
<td> 
SQL </td> <td> FQL </td>
</tr>
<tr>
<td style="vertical-align: top;">

```SQL
SELECT MIN(chn.name) AS character,
       MIN(t.title) AS movie_with_american_producer
FROM char_name AS chn,
     cast_info AS ci,
     company_name AS cn,
     company_type AS ct,
     movie_companies AS mc,
     role_type AS rt,
     title AS t
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[us]'
  AND t.production_year > 1990
  AND t.id = mc.movie_id
  AND t.id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id;


```

</td>
<td style="vertical-align: top;">

```python

input: RF = join(
    DBF(
        {
            "chn": char_name,
            "ci": cast_info.𝛔(note__like="(producer)"),
            "cn": company_name.𝛔(country_code="[us]"),
            "t": title.𝛔(production_year > 1990),
        }
    )
).aggregate(
    character=min("chn.name"),
    movie_with_american_producer=min("t.title"),
)

```

<tr>
<td style="vertical-align: top;">

**SQL Problems:**

1. **redundant join predicates**: join predicates are repeated in the query even though they are implied by the foreign
   object constraints and are inherent and key to the relational model
   declared in
   the schema.
2. **out-of-place filters**: sargable filters (i.e. filters on input relations) are notated out of place: the relation
   is in the FROM-clause, its
   filters in the WHERE clause  (problem: split-attention)
3. **wrong conceptual order**: the order of the statements does not correspond to the conceptual execution order
   places.
4. **repeated the database structure**: some tables that are neither filtered nor used in the aggregates are repeated in
   the query even though they are part of the database schema
5. **separate query language**: SQL is a separate query language that has to be learned and mastered in addition to the
   programming language used for application development, it requires an extra parser and its integration into
   programming languages may lead to SQL injection

<td style="vertical-align: top;">

**FQL Solutions:**

1. **join predicates omitted**: join predicates do not have to be repeated as they are implied by the foreign object
   constraints already declared in
   the schema.
2. **in-place filters**: sargable filters (i.e. filters on input relations) are directly notate with the relation and
   not in two separate places  (spatial contiguity)
3. **correct conceptual order**: the order of the statement corresponds to the conceptual execution order
   places.
4. **no repetition of database structure**: tables that are neither filtered nor used in the aggregates do not have to
   be repeated
   in the query as they are part of the database schema anyway and will be used for query processing as declared in the
   schema
5. **integrated query language**: FQL is integrated into the programming language used for application development, it does not
   require an extra parser and its integration into programming languages makes SQL injection impossible

</td>
</tr>
</table>