import rich_click as click

from .app import Config, JobqueueMonitor


@click.command()
@click.option("--local-port", "local_port", type=int, default=11203)
@click.option("--remote-port", "remote_port", type=int, default=11203)
@click.argument("server", type=str, default=None)
def main(server: str, local_port: int, remote_port: int):
    """monitor the status of a HPC job queue

    SERVER is the SSH host name of the job queue server.
    """
    config = Config(server=server, remote_port=remote_port, local_port=local_port)
    app = JobqueueMonitor(config)
    app.run()
