import rich_click as click


@click.command()
@click.argument("server", type=str, default=None)
def main(server: str):
    """monitor the status of a HPC job queue

    SERVER is the SSH host name of the job queue server.
    """
    pass
