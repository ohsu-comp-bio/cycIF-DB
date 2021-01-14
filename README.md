# cycIF-DB

This library provides simple APIs to manage cycIF quantification data.

##
##### Install postgresql and configure privileges in cloud server(s)

```
ansible-playbook -i playbook/inventory.yml playbook/install_db.yml
```

##### Install dependencies

```
pip install -r requirements.txt
```

##### Deploy database from scratch

```
python scripts/create_db.py
```

##### Upgrade database

```
alembic upgrade head
```

##### Downgrade database

```
alembic downgrade -1
```

##### Autogenerate a revision

```
alembic revision --autogenerate -m "{slug}" --rev-id {id}
```

##### Download quantification datasets from Galaxy

```
python scripts/download_datasets.py {sample_name} {datasets_id} [datasets_id ...]
```

##### Add a sample compex into database

```
python scripts/add_sample_complex.py -d {folder / sample_name}
```
or
```
python scripts/add_sample_complex.py "{sample_name} -- {annotation}" {path_to_cells} {path_to_markers}
```
##
##### Python APIs
```
from cycif_db import CycSession

# list all samples
with CycSession() as csess:
    sample_list = csess.list_samples(detailed=False)
```
