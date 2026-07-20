Star schema and snowflake schema are both dimention models, they both organize data in a fact table and dimension tables. 

The core difference is the way they structure the dimentions

For this project, We better choose the snowflake schema

## Reasons behind choosing Snowflake schema:

we are dealing with a massive amount of data that needs modification and changes frequently
 
we need to minimize redunduncy as much as possible as well as enforcing referencial integrity across master data 

## Why Snoflake helps:
When changing or modifying a dimension, we only apply changes in one row instead of updating each row that references that dimension

Snowflake schema stores each value only once leading to less storage redundancy compared to Star schema 