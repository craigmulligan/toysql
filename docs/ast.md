select a, b from my_table where c=1 order by c limit 1

Select(
  targets=[
    Identifier(parts=['a']),
    Identifier(parts=['b'])
  ],
  from_table=
    Identifier(parts=['my_table']),
  where=
    BinaryOperation(op='=',
      args=(
        Identifier(parts=['c']),
        Constant(value=1)
      )
    ),
  order_by=[
    OrderBy(field=Identifier(parts=['c']), direction='default', nulls='default')
  ],
  limit=Constant(value=1),
)

CREATE TABLE u (id INT, name TEXT)
