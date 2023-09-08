
--create or replace function internal.josh(desc text)
--returns text
--language python
--runtime_version = '3.8'
--handler = 'josh.josh'
--imports = ('{{stage}}/python/josh.py', '{{stage}}/python/crud.py');