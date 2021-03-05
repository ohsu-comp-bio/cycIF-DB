import click

from .cyc_session import CycSession


@click.group()
@click.pass_context
def main(ctx):
    """ Top entry level of cycif database management system.
    """
    pass


@main.command('add_sample_complex')
@click.pass_context
def add_sample_complex(ctx):
    """ Ingest cycif quantification datasets into database.
    """
    pass


@main.command('create_db')
@click.pass_context
def create_db(ctx):
    """ Create database and initiate tables.
    """
    pass


@main.command('download_datasets')
@click.pass_context
def download_datasets(ctx):
    """ Download quantification result datasets from a Galaxy history.
    """
    pass


@main.command('download_sandana_datasets')
@click.pass_context
def download_sandana_datasets(ctx):
    """ Download datasets from SANDANA samples from Galaxy server.
    """
    pass


@main.command('download_shared_histories')
@click.pass_context
def download_shared_histories(ctx):
    """ Download datasets from histories list on /list_shared webpage.
    """
    pass


@main.command('download_tnp_tma_datasets')
@click.pass_context
def download_tnp_tma_datasets(ctx):
    """ Download datasets from histories list on /list_shared webpage.
    """
    pass


@main.command('insert_or_sync_stock_markers')
@click.pass_context
def insert_or_sync_stock_markers(ctx):
    """ Insert or Sync stock markers to database.
    """
    with CycSession() as csess:
        csess.insert_or_sync_markers()


if __name__ == '__main__':
    main()
