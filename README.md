# cycIF-DB

This library provides simple APIs to manage cycIF quantification data.

##
#### Install this library in place

```
pip install -e .
```

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
python scripts/download_datasets.py {sample_name} {datasets_id} [{datasets_id} ...]
```

##### Add a sample compex into database

```
python scripts/add_sample_complex.py -d {folder / sample_name}
```
or
```
python scripts/add_sample_complex.py "{sample_name}__{tag}" {path_to_cells} {path_to_markers}
```
##
#### Python APIs

##### Construct session object

```
from cycif_db import CycSession

csess = CycSession()
```

##### List samples

```
sample_list = csess.list_samples(detailed=False)
```

##### Output a pandas DataFrame for all quantification features associated a sample

```
df = csess.get_cells_for_sample(sample/sample_id, to_path=None)
```

##### Output a pandas DataFrame for all common features associated with multiple samples

```
df = csess.get_cells_from_samples(sample_list/sample_id_list, column_filter='intersection', to_path=None)
```

##### Close session object

```
csess.close()
```
